package org.flexlb.mockengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.grpc.stub.StreamObserver;
import org.flexlb.engine.grpc.EngineRpcService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class JavaMockEngineClusterTest {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final HttpClient HTTP_CLIENT = HttpClient.newHttpClient();

    @TempDir
    Path tempDir;

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);

    @AfterEach
    void tearDown() throws InterruptedException {
        scheduler.shutdownNow();
        scheduler.awaitTermination(2, TimeUnit.SECONDS);
    }

    @Test
    void prefillFormulaUsesAllTasksInBatch() throws Exception {
        MockPerformanceModel model = model(
                "10 + 5*batchSize + 0.01*sum(computeTokens)", 1.0);
        MockLruBlockCache cache = new MockLruBlockCache(100);
        MockPerformanceModel.RequestShape first = model.shape(input(1, 100), cache);
        MockPerformanceModel.RequestShape second = model.shape(input(2, 100), cache);

        long singleMs = model.prefillMs(List.of(first));
        long batchMs = model.prefillMs(List.of(first, second));

        assertTrue(batchMs > singleMs,
                "batch duration must be computed from the complete task set");
    }

    @Test
    void ackIsImmediateWhileQueuedBatchTransitionsFromWaitingToRunningToFinished() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));

        EngineRpcService.EnqueueBatchResponsePB firstAck = enqueue(
                service, batch(11, slot(0, input(1, 100), input(2, 200))));
        EngineRpcService.EnqueueBatchResponsePB secondAck = enqueue(
                service, batch(12, slot(0, input(3, 300), input(4, 400), input(5, 500))));

        assertEquals(2, firstAck.getSuccessesCount());
        assertEquals(3, secondAck.getSuccessesCount());

        EngineRpcService.WorkerStatusPB initial = awaitStatus(service,
                status -> status.getRunningTaskInfoCount() == 5
                        && status.getRunningQueryLen() == 2
                        && status.getWaitingQueryLen() == 3,
                1_000);
        assertEquals(0, initial.getFinishedTaskListCount());
        assertEquals(3, initial.getRunningTaskInfoList().stream()
                .filter(task -> task.getPhase() == EngineRpcService.TaskPhase.TASK_PHASE_RECEIVED)
                .count());
        assertTrue(initial.getRunningTaskInfoList().stream()
                .filter(task -> task.getBatchId() == 12)
                .allMatch(task -> task.getPhase() == EngineRpcService.TaskPhase.TASK_PHASE_RECEIVED));

        EngineRpcService.WorkerStatusPB secondRunning = awaitStatus(service,
                status -> status.getRunningTaskInfoCount() == 3
                        && status.getRunningQueryLen() == 3
                        && status.getWaitingQueryLen() == 0
                        && status.getFinishedTaskListCount() == 2,
                2_000);
        assertTrue(secondRunning.getRunningTaskInfoList().stream()
                .allMatch(task -> task.getBatchId() == 12));

        EngineRpcService.WorkerStatusPB finished = awaitStatus(service,
                status -> status.getRunningTaskInfoCount() == 0
                        && status.getFinishedTaskListCount() == 5,
                2_000);
        assertTrue(finished.getFinishedTaskListList().stream()
                .allMatch(task -> task.getExecutionTimeMs() == 180));
    }

    @Test
    void dpSlotsAreIndependentBatchesWithPerSlotExecutionTime() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("100*batchSize", 1.0));

        enqueue(service, batch(21,
                slot(0, input(1, 100)),
                slot(1, input(2, 100))));

        EngineRpcService.WorkerStatusPB finished = awaitStatus(service,
                status -> status.getFinishedTaskListCount() == 2,
                2_000);
        assertTrue(finished.getFinishedTaskListList().stream()
                .allMatch(task -> task.getExecutionTimeMs() == 100));
        assertEquals(List.of(0L, 1L), finished.getFinishedTaskListList().stream()
                .map(EngineRpcService.TaskInfoPB::getDpRank)
                .sorted()
                .toList());
    }

    // ──────────── Fault injection tests ────────────

    @Test
    void faultInjectionEnqueueErrorReturnsErrors() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .failOnEnqueue(true)
                .enqueueErrorMessage("injected failure")
                .build());

        EngineRpcService.EnqueueBatchResponsePB response = enqueue(
                service, batch(41, slot(0, input(1, 100), input(2, 200))));

        assertEquals(0, response.getSuccessesCount());
        assertEquals(2, response.getErrorsCount());
        assertTrue(response.getErrorsList().stream()
                .allMatch(error -> error.getErrorInfo().getErrorMessage().equals("injected failure")));
    }

    @Test
    void faultInjectionEnqueueDelayDelaysResponse() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .enqueueDelayMs(200)
                .build());

        long start = System.nanoTime();
        EngineRpcService.EnqueueBatchResponsePB response = enqueue(
                service, batch(42, slot(0, input(1, 100))));
        long elapsedMs = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start);

        assertEquals(1, response.getSuccessesCount());
        assertTrue(elapsedMs >= 180, "response should be delayed by ~200ms, got " + elapsedMs + "ms");
    }

    @Test
    void faultInjectionQueueDepthRejectsWhenFull() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("500", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .queueDepthLimit(1)
                .build());

        EngineRpcService.EnqueueBatchResponsePB firstResponse = enqueue(
                service, batch(43, slot(0, input(1, 100))));
        assertEquals(1, firstResponse.getSuccessesCount());

        EngineRpcService.EnqueueBatchResponsePB secondResponse = enqueue(
                service, batch(44, slot(0, input(2, 200))));
        assertEquals(0, secondResponse.getSuccessesCount());
        assertEquals(1, secondResponse.getErrorsCount());
    }

    @Test
    void faultInjectionCrashAfterNStopsEngine() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("500", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .crashAfterNRequests(2)
                .build());

        enqueue(service, batch(45, slot(0, input(1, 100))));
        assertFalse(service.isStopped());

        enqueue(service, batch(46, slot(0, input(2, 200))));
        assertTrue(service.isStopped(), "engine should be stopped after crashAfterNRequests=2");

        EngineRpcService.WorkerStatusPB status = status(service);
        assertFalse(status.getAlive(), "stopped engine should report alive=false");
    }

    @Test
    void kvPressureInflatesKvUsage() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .kvPressureTokens(1_000_000)
                .build());

        EngineRpcService.WorkerStatusPB status = status(service);
        assertTrue(status.getAvailableKvCache() < 6_291_456L,
                "kv pressure should reduce available KV: available=" + status.getAvailableKvCache());
    }

    // ──────────── Cancel test ────────────

    @Test
    void cancelRemovesRequestAndRecordsCancelledCompletion() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("5000", 1.0));

        enqueue(service, batch(51, slot(0, input(1, 100))));

        awaitStatus(service, status -> status.getRunningTaskInfoCount() == 1, 500);

        service.cancel(1);

        EngineRpcService.WorkerStatusPB cancelled = awaitStatus(service,
                status -> status.getRunningTaskInfoCount() == 0
                        && status.getFinishedTaskListCount() >= 1,
                1_000);
        assertTrue(cancelled.getFinishedTaskListList().stream()
                .anyMatch(task -> task.getRequestId() == 1
                        && task.getErrorInfo().getErrorCode() == EngineRpcService.ErrorCodePB.CANCELLED.getNumber()),
                "should have a CANCELLED completion for request 1");
    }

    // ──────────── Inflight drain test ────────────

    @Test
    void inflightDrainsToZeroAfterCompletion() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("50", 1.0));

        enqueue(service, batch(61, slot(0, input(1, 100), input(2, 200))));

        awaitStatus(service, status -> status.getFinishedTaskListCount() == 2, 2_000);

        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(1_000);
        while (System.nanoTime() < deadline) {
            if (service.getInflightCount() == 0) {
                break;
            }
            Thread.sleep(5);
        }
        assertEquals(0, service.getInflightCount(), "inflight should drain to 0 after completion");
        assertFalse(service.isLeakDetected(), "no leak should be detected for normal completion");
    }

    @Test
    void leakDetectionTriggersWhenInflightStuck() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("5000", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .noRespond(true)
                .build());

        enqueue(service, batch(62, slot(0, input(1, 100))));

        awaitStatus(service, status -> status.getRunningTaskInfoCount() == 1, 500);

        service.checkLeakDrain(-1);

        assertTrue(service.isLeakDetected(), "leak should be detected when inflight is stuck");
    }

    // ──────────── HTTP endpoint tests ────────────

    @Test
    void httpSnapshotReturnsAllEngines() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            String body = httpGet(controlServer.getPort(), "/snapshot");
            JsonNode json = MAPPER.readTree(body);
            assertTrue(json.isArray());
            assertEquals(1, json.size());
            JsonNode engine = json.get(0);
            assertEquals(61_000, engine.get("port").asInt());
            assertEquals("prefill", engine.get("role").asText());
            assertFalse(engine.get("stopped").asBoolean());
            assertFalse(engine.get("leak_detected").asBoolean());
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpInjectSetsFaultConfig() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            httpPost(controlServer.getPort(), "/inject",
                    "{\"port\":61000,\"type\":\"enqueue_error\",\"enabled\":true}");
            assertTrue(service.getFaultConfig().isFailOnEnqueue());

            httpPost(controlServer.getPort(), "/inject",
                    "{\"port\":61000,\"type\":\"kv_pressure\",\"enabled\":true,\"tokens\":500000}");
            assertEquals(500_000, service.getFaultConfig().getKvPressureTokens());

            httpPost(controlServer.getPort(), "/inject",
                    "{\"port\":61000,\"type\":\"crash_after\",\"enabled\":true,\"n\":5}");
            assertEquals(5, service.getFaultConfig().getCrashAfterNRequests());
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpClearInjectResetsFaultConfig() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        service.setFaultConfig(FaultInjectionConfig.builder()
                .failOnEnqueue(true)
                .kvPressureTokens(999)
                .build());

        MockControlServer controlServer = startControlServer(service);
        try {
            httpPost(controlServer.getPort(), "/clear_inject", "{\"port\":61000}");
            assertFalse(service.getFaultConfig().isFailOnEnqueue());
            assertEquals(0, service.getFaultConfig().getKvPressureTokens());
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpHealthReturnsEngineCount() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            String body = httpGet(controlServer.getPort(), "/health");
            JsonNode json = MAPPER.readTree(body);
            assertTrue(json.get("healthy").asBoolean());
            assertEquals(1, json.get("engines").asInt());
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpStopEngineReportsNotAlive() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            httpPost(controlServer.getPort(), "/stop_engine", "{\"port\":61000}");
            assertTrue(service.isStopped());

            String healthBody = httpGet(controlServer.getPort(), "/health");
            JsonNode healthJson = MAPPER.readTree(healthBody);
            assertFalse(healthJson.get("healthy").asBoolean());
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpMetricsReturnsPrometheusFormat() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            String body = httpGet(controlServer.getPort(), "/metrics");
            assertTrue(body.contains("mock_engine_running_tasks"));
            assertTrue(body.contains("mock_engine_accepted_total"));
            assertTrue(body.contains("mock_engine_completed_total"));
            assertTrue(body.contains("mock_engine_inflight_count"));
            assertTrue(body.contains("mock_engine_kv_tokens_used"));
            assertTrue(body.contains("mock_engine_heap_used_bytes"));
            assertTrue(body.contains("port=\"61000\""));
        } finally {
            controlServer.stop();
        }
    }

    @Test
    void httpSetPerfModifiesPerformanceModel() throws Exception {
        JavaMockEngineCluster.FastRpcService service = service(model("180", 1.0));
        MockControlServer controlServer = startControlServer(service);
        try {
            httpPost(controlServer.getPort(), "/set_perf",
                    "{\"port\":61000,\"prefill_ms\":42}");
            MockPerformanceModel perf = service.getPerformance();
            MockPerformanceModel.RequestShape shape = perf.shape(input(1, 100), new MockLruBlockCache(100));
            long ms = perf.prefillMs(List.of(shape));
            assertEquals(42, ms, "prefill_ms override should take effect");
        } finally {
            controlServer.stop();
        }
    }

    // ──────────── Helper methods ────────────

    private JavaMockEngineCluster.FastRpcService service(MockPerformanceModel model) {
        Map<Integer, JavaMockEngineCluster.FastRpcService> services = new ConcurrentHashMap<>();
        JavaMockEngineCluster.FastRpcService service = new JavaMockEngineCluster.FastRpcService(
                "prefill",
                EngineRpcService.RoleTypePB.ROLE_TYPE_PREFILL,
                61_000,
                services,
                scheduler,
                model,
                100,
                new JavaMockEngineCluster.ClusterStats());
        services.put(61_000, service);
        return service;
    }

    private MockControlServer startControlServer(JavaMockEngineCluster.FastRpcService service) throws IOException {
        Map<Integer, JavaMockEngineCluster.FastRpcService> services = new ConcurrentHashMap<>();
        services.put(service.getGrpcPort(), service);
        MockControlServer server = new MockControlServer(services, new ConcurrentHashMap<>(), null, null, 0);
        server.start();
        return server;
    }

    private MockPerformanceModel model(String formula, double sleepScale) throws Exception {
        Path performance = tempDir.resolve("performance-" + System.nanoTime() + ".json");
        Path master = tempDir.resolve("master-" + System.nanoTime() + ".json");
        MAPPER.writeValue(performance.toFile(), Map.of(
                "block_size", 1024,
                "sleep_scale", sleepScale,
                "jitter_pct", 0.0,
                "prefill", Map.of("scale", 1.0),
                "decode", Map.of("scale", 1.0, "step_ms_by_batch", List.of(List.of(1, 1.0)))));
        MAPPER.writeValue(master.toFile(), Map.of(
                "zone_process_setting", Map.of(
                        "process_info", Map.of(
                                "envs", List.of(List.of("PREFILL_TIME_FORMULA", formula))))));
        return MockPerformanceModel.load(performance.toString(), master.toString());
    }

    private static EngineRpcService.GenerateInputPB input(long requestId, int inputTokens) {
        EngineRpcService.GenerateInputPB.Builder input = EngineRpcService.GenerateInputPB.newBuilder()
                .setRequestId(requestId)
                .setGenerateConfig(EngineRpcService.GenerateConfigPB.newBuilder()
                        .setMaxNewTokens(1)
                        .build());
        for (int token = 0; token < inputTokens; token++) {
            input.addTokenIds(token);
        }
        return input.build();
    }

    private static EngineRpcService.EnqueueBatchDpSlotPB slot(
            int dpRank, EngineRpcService.GenerateInputPB... inputs) {
        EngineRpcService.EnqueueBatchDpSlotPB.Builder slot =
                EngineRpcService.EnqueueBatchDpSlotPB.newBuilder().setDpRank(dpRank);
        for (EngineRpcService.GenerateInputPB input : inputs) {
            slot.addRequests(EngineRpcService.EnqueueBatchExternalInputPB.newBuilder()
                    .setInput(input)
                    .build());
        }
        return slot.build();
    }

    private static EngineRpcService.EnqueueBatchRequestPB batch(
            long batchId, EngineRpcService.EnqueueBatchDpSlotPB... slots) {
        return EngineRpcService.EnqueueBatchRequestPB.newBuilder()
                .setBatchId(batchId)
                .addAllDpSlots(List.of(slots))
                .build();
    }

    private static EngineRpcService.EnqueueBatchResponsePB enqueue(
            JavaMockEngineCluster.FastRpcService service,
            EngineRpcService.EnqueueBatchRequestPB request) {
        return unary(observer -> service.enqueueBatch(request, observer));
    }

    private static EngineRpcService.WorkerStatusPB status(
            JavaMockEngineCluster.FastRpcService service) {
        return unary(observer -> service.getWorkerStatus(
                EngineRpcService.StatusVersionPB.newBuilder()
                        .setLatestFinishedVersion(0)
                        .build(),
                observer));
    }

    private static EngineRpcService.WorkerStatusPB awaitStatus(
            JavaMockEngineCluster.FastRpcService service,
            Predicate<EngineRpcService.WorkerStatusPB> predicate,
            long timeoutMs) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMs);
        EngineRpcService.WorkerStatusPB last = null;
        while (System.nanoTime() < deadline) {
            last = status(service);
            if (predicate.test(last)) {
                return last;
            }
            Thread.sleep(5);
        }
        fail("status condition not reached, last status=" + last);
        return last;
    }

    private static String httpGet(int port, String path) throws Exception {
        HttpResponse<String> response = HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path))
                        .GET()
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "GET " + path + " failed");
        return response.body();
    }

    private static String httpPost(int port, String path, String body) throws Exception {
        HttpResponse<String> response = HTTP_CLIENT.send(
                HttpRequest.newBuilder()
                        .uri(URI.create("http://127.0.0.1:" + port + path))
                        .header("Content-Type", "application/json")
                        .POST(HttpRequest.BodyPublishers.ofString(body))
                        .build(),
                HttpResponse.BodyHandlers.ofString());
        assertEquals(200, response.statusCode(), "POST " + path + " failed");
        return response.body();
    }

    private static <T> T unary(Consumer<StreamObserver<T>> invocation) {
        AtomicReference<T> response = new AtomicReference<>();
        AtomicReference<Throwable> error = new AtomicReference<>();
        CountDownLatch latch = new CountDownLatch(1);
        invocation.accept(new StreamObserver<>() {
            @Override
            public void onNext(T value) {
                response.set(value);
            }

            @Override
            public void onError(Throwable throwable) {
                error.set(throwable);
                latch.countDown();
            }

            @Override
            public void onCompleted() {
                latch.countDown();
            }
        });
        try {
            if (!latch.await(5, TimeUnit.SECONDS)) {
                fail("unary response timeout");
            }
        } catch (InterruptedException e) {
            fail("interrupted waiting for unary response");
        }
        if (error.get() != null) {
            throw new AssertionError(error.get());
        }
        assertNotNull(response.get(), "unary response");
        return response.get();
    }
}
