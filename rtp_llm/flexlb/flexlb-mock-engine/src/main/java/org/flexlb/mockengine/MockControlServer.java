package org.flexlb.mockengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import io.grpc.Server;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

/**
 * Lightweight HTTP control server for the Java mock engine cluster.
 *
 * <p>Provides 11 endpoints mirroring the Python mock_engine.py control API:
 * snapshot, inject, clear_inject, health, requests, set_perf, set_kv_pressure,
 * set_queue_depth, stop_engine, start_engine, and metrics.
 *
 * <p>Uses JDK built-in {@link HttpServer} — no additional Maven dependencies.
 */
final class MockControlServer {

    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final HttpServer httpServer;
    private final Map<Integer, JavaMockEngineCluster.FastRpcService> services;
    private final Map<Integer, Server> serversByPort;
    private final EventLoopGroup bossGroup;
    private final EventLoopGroup workerGroup;

    MockControlServer(Map<Integer, JavaMockEngineCluster.FastRpcService> services,
                      Map<Integer, Server> serversByPort,
                      EventLoopGroup bossGroup,
                      EventLoopGroup workerGroup,
                      int httpPort) throws IOException {
        this.services = services;
        this.serversByPort = serversByPort;
        this.bossGroup = bossGroup;
        this.workerGroup = workerGroup;
        this.httpServer = HttpServer.create(new InetSocketAddress(httpPort), 0);
        httpServer.createContext("/snapshot", this::handleSnapshot);
        httpServer.createContext("/inject", this::handleInject);
        httpServer.createContext("/clear_inject", this::handleClearInject);
        httpServer.createContext("/health", this::handleHealth);
        httpServer.createContext("/requests", this::handleRequests);
        httpServer.createContext("/set_perf", this::handleSetPerf);
        httpServer.createContext("/set_kv_pressure", this::handleSetKvPressure);
        httpServer.createContext("/set_queue_depth", this::handleSetQueueDepth);
        httpServer.createContext("/stop_engine", this::handleStopEngine);
        httpServer.createContext("/start_engine", this::handleStartEngine);
        httpServer.createContext("/metrics", this::handleMetrics);
        httpServer.setExecutor(Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, "mock-control-http");
            t.setDaemon(true);
            return t;
        }));
    }

    void start() {
        httpServer.start();
    }

    void stop() {
        httpServer.stop(0);
    }

    int getPort() {
        return httpServer.getAddress().getPort();
    }

    // ────────────────── Endpoint handlers ──────────────────

    private void handleSnapshot(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        List<Map<String, Object>> engines = new ArrayList<>();
        for (JavaMockEngineCluster.FastRpcService service : services.values()) {
            engines.add(service.getSnapshot());
        }
        sendJson(exchange, 200, engines);
    }

    private void handleInject(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();
        String type = body.path("type").asText();
        boolean enabled = body.path("enabled").asBoolean(true);

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }

        FaultInjectionConfig.Builder builder = service.getFaultConfig().toBuilder();
        switch (type) {
            case "enqueue_error" -> builder.failOnEnqueue(enabled);
            case "generate_error" -> builder.generateError(enabled);
            case "fetch_error" -> builder.fetchError(enabled);
            case "no_respond" -> builder.noRespond(enabled);
            case "kv_pressure" -> builder.kvPressureTokens(enabled ? body.path("tokens").asLong(500_000) : 0);
            case "queue_depth" -> builder.queueDepthLimit(enabled ? body.path("depth").asInt(10) : 0);
            case "crash_after" -> builder.crashAfterNRequests(enabled ? body.path("n").asInt(5) : 0);
            default -> {
                sendJson(exchange, 400, Map.of("error", "unknown injection type: " + type));
                return;
            }
        }
        service.setFaultConfig(builder.build());
        sendJson(exchange, 200, Map.of("status", "ok", "port", port, "type", type));
    }

    private void handleClearInject(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        service.clearFaultConfig();
        sendJson(exchange, 200, Map.of("status", "ok", "port", port));
    }

    private void handleHealth(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        int total = services.size();
        long healthy = services.values().stream().filter(s -> !s.isStopped()).count();
        Map<String, Object> response = new LinkedHashMap<>();
        response.put("healthy", healthy == total);
        response.put("engines", total);
        sendJson(exchange, 200, response);
    }

    private void handleRequests(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        List<Map<String, Object>> requests = new ArrayList<>();
        for (JavaMockEngineCluster.FastRpcService service : services.values()) {
            for (Map.Entry<Long, String> entry : service.getRequestStates().entrySet()) {
                Map<String, Object> req = new LinkedHashMap<>();
                req.put("request_id", entry.getKey());
                req.put("state", entry.getValue());
                req.put("port", service.getGrpcPort());
                req.put("role", service.getRoleName());
                requests.add(req);
            }
        }
        sendJson(exchange, 200, requests);
    }

    private void handleSetPerf(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        MockPerformanceModel perf = service.getPerformance();
        if (body.has("prefill_ms")) {
            perf.setOverrideFixedPrefillMs(body.get("prefill_ms").asDouble());
        }
        if (body.has("decode_step_ms")) {
            perf.setOverrideDecodeStepMs(body.get("decode_step_ms").asDouble());
        }
        if (body.has("jitter_pct")) {
            perf.setJitterPct(body.get("jitter_pct").asDouble());
        }
        sendJson(exchange, 200, Map.of("status", "ok", "port", port));
    }

    private void handleSetKvPressure(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        long tokens = body.path("tokens").asLong(0);
        FaultInjectionConfig.Builder builder = service.getFaultConfig().toBuilder();
        builder.kvPressureTokens(tokens);
        service.setFaultConfig(builder.build());
        sendJson(exchange, 200, Map.of("status", "ok", "port", port));
    }

    private void handleSetQueueDepth(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        int depth = body.path("depth").asInt(0);
        FaultInjectionConfig.Builder builder = service.getFaultConfig().toBuilder();
        builder.queueDepthLimit(depth);
        service.setFaultConfig(builder.build());
        sendJson(exchange, 200, Map.of("status", "ok", "port", port));
    }

    private void handleStopEngine(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        service.setStopped(true);
        Server server = serversByPort.get(port);
        if (server != null) {
            server.shutdownNow();
        }
        sendJson(exchange, 200, Map.of("status", "ok", "port", port, "action", "stopped"));
    }

    private void handleStartEngine(HttpExchange exchange) throws IOException {
        if (!"POST".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        JsonNode body = MAPPER.readTree(exchange.getRequestBody());
        int port = body.path("port").asInt();

        JavaMockEngineCluster.FastRpcService service = services.get(port);
        if (service == null) {
            sendJson(exchange, 404, Map.of("error", "engine not found for port " + port));
            return;
        }
        service.setStopped(false);
        Server existing = serversByPort.get(port);
        if (existing != null && !existing.isShutdown()) {
            existing.shutdownNow();
        }
        Server server = NettyServerBuilder.forPort(port)
                .bossEventLoopGroup(bossGroup)
                .workerEventLoopGroup(workerGroup)
                .channelType(NioServerSocketChannel.class)
                .directExecutor()
                .maxInboundMessageSize(16 * 1024 * 1024)
                .addService(service)
                .build()
                .start();
        serversByPort.put(port, server);
        sendJson(exchange, 200, Map.of("status", "ok", "port", port, "action", "started"));
    }

    private void handleMetrics(HttpExchange exchange) throws IOException {
        if (!"GET".equals(exchange.getRequestMethod())) {
            sendJson(exchange, 405, Map.of("error", "Method Not Allowed"));
            return;
        }
        String[] metricDefs = {
                "mock_engine_running_tasks", "gauge", "Current running tasks",
                "mock_engine_accepted_total", "counter", "Total accepted requests",
                "mock_engine_completed_total", "counter", "Total completed requests",
                "mock_engine_inflight_count", "gauge", "Current inflight count",
                "mock_engine_kv_tokens_used", "gauge", "KV cache tokens in use",
                "mock_engine_heap_used_bytes", "gauge", "JVM heap used in bytes"
        };
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < metricDefs.length; i += 3) {
            sb.append("# HELP ").append(metricDefs[i]).append(' ').append(metricDefs[i + 2]).append('\n');
            sb.append("# TYPE ").append(metricDefs[i]).append(' ').append(metricDefs[i + 1]).append('\n');
        }
        Runtime runtime = Runtime.getRuntime();
        long heapUsed = runtime.totalMemory() - runtime.freeMemory();
        for (JavaMockEngineCluster.FastRpcService service : services.values()) {
            String labels = String.format("port=\"%d\",role=\"%s\"",
                    service.getGrpcPort(), service.getRoleName());
            sb.append(String.format("mock_engine_running_tasks{%s} %d%n", labels, service.getRunningCount()));
            sb.append(String.format("mock_engine_accepted_total{%s} %d%n", labels, service.getAcceptedCount()));
            sb.append(String.format("mock_engine_completed_total{%s} %d%n", labels, service.getCompletedCount()));
            sb.append(String.format("mock_engine_inflight_count{%s} %d%n", labels, service.getInflightCount()));
            sb.append(String.format("mock_engine_kv_tokens_used{%s} %d%n", labels, service.getActiveKvTokens()));
            sb.append(String.format("mock_engine_heap_used_bytes{%s} %d%n", labels, heapUsed));
        }
        sendText(exchange, 200, sb.toString());
    }

    // ────────────────── Utility methods ──────────────────

    private static void sendJson(HttpExchange exchange, int status, Object body) throws IOException {
        String json = MAPPER.writeValueAsString(body);
        byte[] bytes = json.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }

    private static void sendText(HttpExchange exchange, int status, String text) throws IOException {
        byte[] bytes = text.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", "text/plain; version=0.0.4; charset=utf-8");
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream os = exchange.getResponseBody()) {
            os.write(bytes);
        }
    }
}
