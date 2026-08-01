package org.flexlb.mockengine;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import io.grpc.ManagedChannel;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.RpcServiceGrpc;
import org.flexlb.schedule.grpc.FlexlbScheduleProtocol;
import org.flexlb.schedule.grpc.FlexlbServiceGrpc;

import java.io.BufferedWriter;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Standalone Java load client replacing flexlb_load_client.py.
 *
 * <p>Replays trace JSONL files against a running FlexLB master via gRPC Schedule RPC.
 * Supports multi-shard replay, configurable speed, semaphore-based concurrency control,
 * optional engine stream reading for TTFT/total latency, and generates summary.json +
 * per_request.jsonl matching the Python client format.
 *
 * <p>Configuration is read exclusively from environment variables at startup (no
 * multi-layer override). Run as:
 * <pre>{@code
 *   java -cp <jar> org.flexlb.mockengine.JavaLoadClient
 * }</pre>
 */
public final class JavaLoadClient {

    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final int BLOCK_SIZE = 1024;

    private final Config config;
    private final EventLoopGroup eventLoopGroup;
    private final ManagedChannel[] scheduleChannels;
    private final FlexlbServiceGrpc.FlexlbServiceBlockingStub[] scheduleStubs;
    private final AtomicInteger scheduleStubRR = new AtomicInteger();
    private final Map<String, ManagedChannel[]> engineChannelPools = new ConcurrentHashMap<>();
    private final Map<String, AtomicInteger> engineChannelRR = new ConcurrentHashMap<>();
    private final HttpClient httpClient = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(5)).build();
    private final List<RequestResult> results = new ArrayList<>();
    private final AtomicInteger actualSentCount = new AtomicInteger();
    private final AtomicInteger responseCount = new AtomicInteger();
    private volatile long replayStartedEpochMs;
    private volatile long replayStartedNanos;
    private volatile long sendStartNanos;
    private volatile long sendEndNanos;

    private JavaLoadClient(Config config) {
        this.config = config;
        this.eventLoopGroup = new NioEventLoopGroup(config.eventLoopThreads);
        this.scheduleChannels = new ManagedChannel[config.nChannels];
        this.scheduleStubs = new FlexlbServiceGrpc.FlexlbServiceBlockingStub[config.nChannels];
        for (int i = 0; i < config.nChannels; i++) {
            ManagedChannel channel = NettyChannelBuilder.forTarget(config.grpcTarget)
                    .eventLoopGroup(eventLoopGroup)
                    .channelType(NioSocketChannel.class)
                    .maxInboundMessageSize(16 * 1024 * 1024)
                    .flowControlWindow(1024 * 1024)
                    .keepAliveTime(30, TimeUnit.SECONDS)
                    .keepAliveTimeout(10, TimeUnit.SECONDS)
                    .usePlaintext()
                    .build();
            scheduleChannels[i] = channel;
            scheduleStubs[i] = FlexlbServiceGrpc.newBlockingStub(channel);
        }
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        config.print();
        if (config.traceFile.isEmpty()) {
            throw new IllegalArgumentException("TRACE_FILE environment variable is required");
        }
        JavaLoadClient client = new JavaLoadClient(config);
        try {
            client.run();
        } finally {
            client.close();
        }
    }

    void run() throws Exception {
        List<TraceRecord> records = loadTrace(config.traceFile);
        if (records.isEmpty()) {
            throw new RuntimeException("no replayable requests loaded from " + config.traceFile);
        }

        if (config.numShards > 1) {
            if (config.shardIndex < 0 || config.shardIndex >= config.numShards) {
                throw new IllegalArgumentException("SHARD_INDEX must be in [0, NUM_SHARDS)");
            }
            List<TraceRecord> sharded = new ArrayList<>();
            for (int i = 0; i < records.size(); i++) {
                if (i % config.numShards == config.shardIndex) {
                    sharded.add(records.get(i));
                }
            }
            records = sharded;
            if (records.isEmpty()) {
                throw new RuntimeException("trace shard has no replayable requests");
            }
        }

        if (config.durationS > 0 && !config.loop) {
            long firstTs = records.get(0).tsMs;
            long endTs = firstTs + config.durationS * 1000L;
            records = records.stream().filter(r -> r.tsMs <= endTs).toList();
        }
        if (config.limit > 0 && !config.loop) {
            records = records.subList(0, Math.min(config.limit, records.size()));
        }

        System.out.println("loaded " + records.size() + " requests from " + config.traceFile
                + " (shard=" + config.shardIndex + "/" + config.numShards + ")");

        Files.createDirectories(Path.of(config.outputDir));
        if (!config.skipServerLatency) {
            resetServerLatency();
        }

        long firstTsMs = records.get(0).tsMs;
        long traceSpanMs = Math.max(records.get(records.size() - 1).tsMs - firstTsMs, 1);

        if (config.startAtEpochMs > 0) {
            long waitMs = config.startAtEpochMs - System.currentTimeMillis();
            if (waitMs > 0) {
                System.out.println("waiting " + waitMs + "ms for start barrier...");
                Thread.sleep(waitMs);
            }
        }

        replayStartedEpochMs = System.currentTimeMillis();
        replayStartedNanos = System.nanoTime();
        sendStartNanos = replayStartedNanos;

        Semaphore semaphore = new Semaphore(config.maxConcurrency);
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        List<Future<RequestResult>> futures = new ArrayList<>();
        int sentCount = 0;
        int loopIdx = 0;

        while (true) {
            for (TraceRecord record : records) {
                if (config.loop && config.durationS > 0) {
                    if ((System.nanoTime() - replayStartedNanos) / 1_000_000_000L >= config.durationS) {
                        break;
                    }
                }
                if (config.limit > 0 && sentCount >= config.limit) {
                    break;
                }

                double dueSeconds = 0;
                if (config.replaySpeed > 0 && record.tsMs > 0) {
                    long loopOffsetMs = (long) loopIdx * traceSpanMs;
                    dueSeconds = (record.tsMs - firstTsMs + loopOffsetMs) / 1000.0 / config.replaySpeed;
                    long dueNanos = replayStartedNanos + (long) (dueSeconds * 1_000_000_000L);
                    long sleepNanos = dueNanos - System.nanoTime();
                    if (sleepNanos > 0) {
                        Thread.sleep(sleepNanos / 1_000_000, (int) (sleepNanos % 1_000_000));
                    }
                }

                if (config.loop && config.durationS > 0) {
                    if ((System.nanoTime() - replayStartedNanos) / 1_000_000_000L >= config.durationS) {
                        break;
                    }
                }

                final TraceRecord loopRecord;
                if (loopIdx > 0) {
                    loopRecord = makeLoopRequest(record, loopIdx, sentCount);
                } else {
                    loopRecord = record;
                }
                final double dueS = dueSeconds;
                futures.add(executor.submit(() -> handleRequest(loopRecord, semaphore, dueS)));
                sentCount++;
            }

            if (!config.loop) {
                break;
            }
            if (config.durationS > 0
                    && (System.nanoTime() - replayStartedNanos) / 1_000_000_000L >= config.durationS) {
                break;
            }
            if (config.limit > 0 && sentCount >= config.limit) {
                break;
            }
            loopIdx++;
            if (futures.size() >= 100_000) {
                // Collect results from completed futures before removing them
                // to avoid losing latency/error statistics in loop mode.
                futures.removeIf(future -> {
                    if (future.isDone()) {
                        try {
                            RequestResult collected = future.get();
                            if (collected != null) {
                                results.add(collected);
                            }
                        } catch (Exception ignored) {
                            // Result could not be retrieved; will not be counted.
                        }
                        return true;
                    }
                    return false;
                });
            }
            System.out.println("loop replay: iteration " + loopIdx + " starting, sent " + sentCount
                    + " requests, elapsed " + (System.nanoTime() - replayStartedNanos) / 1_000_000_000L + "s");
        }

        sendEndNanos = System.nanoTime();
        double sendDurationS = (sendEndNanos - sendStartNanos) / 1_000_000_000.0;
        System.out.println("sending complete: sent=" + sentCount + " requests dispatched in "
                + String.format("%.1f", sendDurationS) + "s, waiting for responses...");

        ScheduledExecutorService progressMonitor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "progress-monitor");
            t.setDaemon(true);
            return t;
        });
        final int totalSent = sentCount;
        progressMonitor.scheduleAtFixedRate(() -> {
            System.out.println("  progress: " + responseCount.get() + "/" + totalSent
                    + " responses received, elapsed "
                    + String.format("%.1f", (System.nanoTime() - replayStartedNanos) / 1_000_000_000.0) + "s");
        }, 10, 10, TimeUnit.SECONDS);

        long deadlineNanos = System.nanoTime()
                + TimeUnit.SECONDS.toNanos(config.responseTimeoutSeconds);
        for (int i = 0; i < futures.size(); i++) {
            long remaining = deadlineNanos - System.nanoTime();
            if (remaining <= 0) {
                futures.get(i).cancel(true);
                // Count timed-out requests as errors so they are reflected in error_count
                RequestResult timeoutResult = new RequestResult();
                timeoutResult.status = "timeout";
                timeoutResult.error = "response deadline exceeded";
                results.add(timeoutResult);
                continue;
            }
            try {
                RequestResult result = futures.get(i).get(remaining, TimeUnit.NANOSECONDS);
                results.add(result);
            } catch (java.util.concurrent.TimeoutException e) {
                futures.get(i).cancel(true);
                RequestResult timeoutResult = new RequestResult();
                timeoutResult.status = "timeout";
                timeoutResult.error = "response timeout";
                results.add(timeoutResult);
            } catch (Exception e) {
                futures.get(i).cancel(true);
                RequestResult errorResult = new RequestResult();
                errorResult.status = "exception";
                errorResult.error = e.toString();
                results.add(errorResult);
            }
        }
        progressMonitor.shutdownNow();
        executor.shutdownNow();

        long elapsedNanos = System.nanoTime() - replayStartedNanos;
        double elapsedS = elapsedNanos / 1_000_000_000.0;
        System.out.println("responses collected: " + results.size() + "/" + sentCount
                + " in " + String.format("%.1f", elapsedS) + "s");

        JsonNode serverLatency = config.skipServerLatency ? MAPPER.createObjectNode() : fetchServerLatency();
        writePerRequestResults();
        writeSummary(serverLatency, elapsedS, sendDurationS, sentCount);
    }

    private TraceRecord makeLoopRequest(TraceRecord req, int loopIdx, int sentCount) {
        String newSourceRid = req.sourceRid + "_L" + loopIdx;
        String newTraceId = req.traceId.isEmpty() ? "" : req.traceId + "_L" + loopIdx;
        long newRequestId = stableRequestId(newSourceRid);
        return new TraceRecord(newRequestId, newSourceRid, newTraceId, req.tsMs,
                req.inputLen, req.outputLen, req.blockKeys, req.tokenIds);
    }

    private RequestResult handleRequest(TraceRecord record, Semaphore semaphore, double dueS) {
        long startedNanos = System.nanoTime();
        double sendDueEpochMs = replayStartedEpochMs + dueS * 1000.0;

        RequestResult result = new RequestResult();
        result.rid = record.sourceRid;
        result.traceId = record.traceId;
        result.requestId = record.requestId;
        result.ts = record.tsMs;
        result.inputLen = record.inputLen;
        result.outputLen = record.outputLen;
        result.status = "unknown";
        result.routePath = "master";
        result.sendDueEpochMs = sendDueEpochMs;

        EngineRpcService.GenerateInputPB inputPb = null;
        FlexlbScheduleProtocol.FlexlbScheduleResponsePB scheduleResponse = null;

        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            result.status = "exception";
            result.error = "interrupted";
            result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
            responseCount.incrementAndGet();
            return result;
        }

        try {
            double sendStartEpochMs = replayStartedEpochMs
                    + (System.nanoTime() - replayStartedNanos) / 1_000_000.0;
            result.sendStartEpochMs = sendStartEpochMs;
            result.pacingLagMs = Math.max(0.0, sendStartEpochMs - sendDueEpochMs);
            actualSentCount.incrementAndGet();

            inputPb = buildGenerateInput(record);
            FlexlbScheduleProtocol.FlexlbScheduleRequestPB scheduleReq = buildScheduleRequest(record, inputPb);

            long scheduleStartNanos = System.nanoTime();
            FlexlbServiceGrpc.FlexlbServiceBlockingStub stub = nextScheduleStub()
                    .withDeadlineAfter(config.timeoutMs, TimeUnit.MILLISECONDS);
            scheduleResponse = stub.schedule(scheduleReq);

            result.scheduleMs = (System.nanoTime() - scheduleStartNanos) / 1_000_000.0;
            result.enqueuedByMaster = scheduleResponse.getEnqueuedByMaster();

            if (scheduleResponse.getCode() != 200 || !scheduleResponse.getSuccess()) {
                result.status = "schedule_error";
                result.error = scheduleResponse.getErrorMessage().isEmpty()
                        ? "code=" + scheduleResponse.getCode()
                        : scheduleResponse.getErrorMessage();
                result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
                responseCount.incrementAndGet();
                return result;
            }

            result.prefill = roleAddr(scheduleResponse, "PREFILL");
            result.decode = roleAddr(scheduleResponse, "DECODE");

            if (!config.fetchResponseEnabled) {
                result.status = "scheduled";
                result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
                responseCount.incrementAndGet();
                return result;
            }
        } catch (Exception e) {
            result.status = "exception";
            result.error = e.toString();
            result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
            responseCount.incrementAndGet();
            return result;
        } finally {
            semaphore.release();
        }

        // Phase 2: engine stream reading (outside semaphore)
        if (scheduleResponse != null && config.fetchResponseEnabled) {
            try {
                Double firstFrameNanos = null;
                Double terminalNanos = null;

                String prefillAddr = roleAddr(scheduleResponse, "PREFILL");
                if (prefillAddr.isEmpty()) {
                    prefillAddr = roleAddr(scheduleResponse, "PDFUSION");
                }
                if (prefillAddr.isEmpty()) {
                    throw new RuntimeException("schedule response has no PREFILL/PDFUSION address");
                }

                EngineRpcService.GenerateInputPB modifiedInput = copyRoleAddrs(inputPb, scheduleResponse);
                ManagedChannel engineChannel = getEngineChannel(prefillAddr);
                RpcServiceGrpc.RpcServiceBlockingStub engineStub = RpcServiceGrpc.newBlockingStub(engineChannel)
                        .withDeadlineAfter(config.timeoutMs, TimeUnit.MILLISECONDS);

                Iterator<EngineRpcService.GenerateOutputsPB> stream;
                if (scheduleResponse.getEnqueuedByMaster()) {
                    stream = engineStub.fetchResponse(EngineRpcService.FetchRequestPB.newBuilder()
                            .setRequestId(inputPb.getRequestId())
                            .build());
                } else {
                    stream = engineStub.generateStreamCall(modifiedInput);
                }

                while (stream.hasNext()) {
                    EngineRpcService.GenerateOutputsPB output = stream.next();
                    long now = System.nanoTime();
                    if (firstFrameNanos == null) {
                        firstFrameNanos = (double) now;
                    }
                    EngineRpcService.FlattenOutputPB flatten = output.getFlattenOutput();
                    for (int j = 0; j < flatten.getFinishedCount(); j++) {
                        if (flatten.getFinished(j)) {
                            terminalNanos = (double) now;
                        }
                    }
                }

                if (firstFrameNanos == null) {
                    // Stream completed with zero outputs — mark as error to avoid
                    // masking underlying engine issues as successful requests.
                    result.status = "empty_response";
                    result.error = "stream completed with zero outputs";
                    result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
                } else {
                    long endNanos = terminalNanos != null ? terminalNanos.longValue() : System.nanoTime();
                    result.ttftMs = (firstFrameNanos - startedNanos) / 1_000_000.0;
                    result.totalMs = (endNanos - startedNanos) / 1_000_000.0;
                    result.status = "ok";
                }
                result.wallClockTs = System.currentTimeMillis() / 1000.0;
            } catch (Exception e) {
                result.status = "exception";
                result.error = e.toString();
                result.totalMs = (System.nanoTime() - startedNanos) / 1_000_000.0;
            }
        }

        responseCount.incrementAndGet();
        return result;
    }

    private EngineRpcService.GenerateInputPB buildGenerateInput(TraceRecord record) throws IOException {
        ObjectNode meta = MAPPER.createObjectNode();
        meta.put("rid", record.sourceRid);
        meta.put("trace_id", record.traceId);
        meta.put("input_len", record.inputLen);
        meta.put("output_len", record.outputLen);
        ArrayNode keysArray = meta.putArray("block_cache_keys");
        for (long key : record.blockKeys) {
            keysArray.add(key);
        }
        String uniqueKey = "flexlb_eval:" + MAPPER.writeValueAsString(meta);

        EngineRpcService.GenerateConfigPB.Builder genConfig = EngineRpcService.GenerateConfigPB.newBuilder()
                .setMaxNewTokens(Math.max(1, record.outputLen))
                .setNumReturnSequences(1)
                .setTopP(1.0f)
                .setTopK(0)
                .setTemperature(1.0f)
                .setReturnIncremental(true)
                .setIsStreaming(true)
                .setTimeoutMs((int) Math.min(config.timeoutMs, Integer.MAX_VALUE))
                .setUniqueKey(uniqueKey);

        EngineRpcService.RequestInfoPB.Builder info = EngineRpcService.RequestInfoPB.newBuilder()
                .setRequestId(record.sourceRid)
                .setTraceId(record.traceId)
                .setSourceRole("flexlb_eval");

        EngineRpcService.GenerateInputPB.Builder input = EngineRpcService.GenerateInputPB.newBuilder()
                .setRequestId(record.requestId)
                .addAllTokenIds(record.tokenIds)
                .setGenerateConfig(genConfig)
                .setClientId("flexlb_eval_client")
                .setStartTime(System.currentTimeMillis())
                .setRequestInfo(info);

        return input.build();
    }

    private FlexlbScheduleProtocol.FlexlbScheduleRequestPB buildScheduleRequest(
            TraceRecord record, EngineRpcService.GenerateInputPB inputPb) {
        return FlexlbScheduleProtocol.FlexlbScheduleRequestPB.newBuilder()
                .setRequestId(record.requestId)
                .setGenerateInput(inputPb.toByteString())
                .addAllBlockCacheKeys(record.blockKeys)
                .setSeqLen(record.inputLen)
                .setGenerateTimeout(config.timeoutMs)
                .setRequestTimeMs(System.currentTimeMillis())
                .setMaxNewTokens(Math.max(1, record.outputLen))
                .setNumBeams(1)
                .setForceDisableSpRun(false)
                .setModel(config.model)
                .setApiKey(config.apiKey)
                .setCacheKeyBlockSize(BLOCK_SIZE)
                .build();
    }

    private EngineRpcService.GenerateInputPB copyRoleAddrs(
            EngineRpcService.GenerateInputPB inputPb,
            FlexlbScheduleProtocol.FlexlbScheduleResponsePB response) {
        EngineRpcService.GenerateInputPB.Builder modified = inputPb.toBuilder();
        modified.getGenerateConfigBuilder().clearRoleAddrs();
        for (FlexlbScheduleProtocol.FlexlbServerStatusPB status : response.getServerStatusList()) {
            EngineRpcService.RoleTypePB roleType = switch (status.getRole()) {
                case "PREFILL" -> EngineRpcService.RoleTypePB.ROLE_TYPE_PREFILL;
                case "DECODE" -> EngineRpcService.RoleTypePB.ROLE_TYPE_DECODE;
                default -> EngineRpcService.RoleTypePB.ROLE_TYPE_PDFUSION;
            };
            modified.getGenerateConfigBuilder().addRoleAddrs(
                    EngineRpcService.RoleAddrPB.newBuilder()
                            .setRole(status.getRole())
                            .setRoleType(roleType)
                            .setIp(status.getServerIp())
                            .setHttpPort(status.getHttpPort())
                            .setGrpcPort(status.getGrpcPort())
                            .build());
        }
        return modified.build();
    }

    private String roleAddr(FlexlbScheduleProtocol.FlexlbScheduleResponsePB response, String role) {
        for (FlexlbScheduleProtocol.FlexlbServerStatusPB status : response.getServerStatusList()) {
            if (status.getRole().equals(role) && !status.getServerIp().isEmpty()) {
                return status.getServerIp() + ":" + status.getGrpcPort();
            }
        }
        return "";
    }

    private FlexlbServiceGrpc.FlexlbServiceBlockingStub nextScheduleStub() {
        int idx = scheduleStubRR.getAndIncrement() % config.nChannels;
        return scheduleStubs[Math.floorMod(idx, config.nChannels)];
    }

    private ManagedChannel getEngineChannel(String target) {
        ManagedChannel[] pool = engineChannelPools.computeIfAbsent(target, t -> {
            ManagedChannel[] channels = new ManagedChannel[config.nChannels];
            for (int i = 0; i < config.nChannels; i++) {
                channels[i] = NettyChannelBuilder.forTarget(t)
                        .eventLoopGroup(eventLoopGroup)
                        .channelType(NioSocketChannel.class)
                        .maxInboundMessageSize(16 * 1024 * 1024)
                        .flowControlWindow(1024 * 1024)
                        .keepAliveTime(30, TimeUnit.SECONDS)
                        .keepAliveTimeout(10, TimeUnit.SECONDS)
                        .usePlaintext()
                        .build();
            }
            engineChannelRR.put(t, new AtomicInteger());
            return channels;
        });
        AtomicInteger rr = engineChannelRR.get(target);
        int idx = Math.floorMod(rr.getAndIncrement(), config.nChannels);
        return pool[idx];
    }

    // ---- Trace Loading ----

    private List<TraceRecord> loadTrace(String path) throws IOException {
        List<TraceRecord> records = new ArrayList<>();
        for (String line : Files.readAllLines(Path.of(path))) {
            if (line.isBlank()) {
                continue;
            }
            try {
                JsonNode raw = MAPPER.readTree(line);
                TraceRecord record = parseTraceRecord(raw);
                if (record != null) {
                    records.add(record);
                }
            } catch (Exception e) {
                System.err.println("skipping malformed trace line: " + e.getMessage());
            }
        }
        records.sort(Comparator.comparingLong(r -> r.tsMs));
        return records;
    }

    private TraceRecord parseTraceRecord(JsonNode raw) {
        int inputLen = raw.path("il").asInt(raw.path("input_token_len")
                .asInt(raw.path("backend_input_token_len").asInt(0)));
        if (inputLen <= 0) {
            return null;
        }

        int outputLen = raw.path("ol").asInt(raw.path("output_token_len").asInt(0));
        if (outputLen <= 0) {
            if ("skip".equals(config.zeroOutputPolicy)) {
                return null;
            } else if ("one".equals(config.zeroOutputPolicy)) {
                outputLen = 1;
            } else if ("default100".equals(config.zeroOutputPolicy)) {
                outputLen = 100;
            }
        }

        String sourceRid = raw.has("request_id") ? raw.get("request_id").asText()
                : raw.has("rid") ? raw.get("rid").asText()
                : Long.toString(stableRequestId(raw.toString()));
        String traceId = extractTraceId(raw);
        if (traceId.isEmpty()) {
            traceId = sourceRid;
        }

        long requestId;
        JsonNode ridIntNode = raw.get("request_id_int");
        if (ridIntNode != null) {
            requestId = toSignedInt64(ridIntNode.bigIntegerValue());
        } else {
            requestId = stableRequestId(sourceRid);
        }

        long tsMs = raw.path("ts").asLong(raw.path("request_enter_ts_epoch_ms")
                .asLong(raw.path("ts_epoch_ms").asLong(0)));

        List<Integer> tokenIds = null;
        JsonNode inputIdsNode = raw.get("input_ids");
        if (inputIdsNode != null && inputIdsNode.isArray()) {
            tokenIds = new ArrayList<>(inputIdsNode.size());
            for (JsonNode token : inputIdsNode) {
                tokenIds.add(token.asInt());
            }
        } else {
            tokenIds = Collections.nCopies(inputLen, 0);
        }

        List<Long> blockKeys = new ArrayList<>();
        JsonNode bhNode = raw.get("bh");
        if (bhNode == null) {
            bhNode = raw.get("block_cache_keys");
        }
        if (bhNode != null && bhNode.isArray()) {
            for (JsonNode key : bhNode) {
                blockKeys.add(toSignedInt64(key.bigIntegerValue()));
            }
        } else if (tokenIds != null) {
            blockKeys = computeBlockKeys(tokenIds, BLOCK_SIZE);
        }

        return new TraceRecord(requestId, sourceRid, traceId, tsMs,
                inputLen, outputLen, blockKeys, tokenIds);
    }

    private String extractTraceId(JsonNode raw) {
        JsonNode traceIdNode = raw.get("trace_id");
        if (traceIdNode != null && !traceIdNode.asText().isEmpty()) {
            return traceIdNode.asText();
        }
        JsonNode controls = raw.get("request_controls");
        if (controls == null || !controls.isObject()) {
            return "";
        }
        JsonNode params = controls.get("parameters");
        if (params != null && params.isObject()) {
            for (String key : List.of("trace_id", "traceparent")) {
                JsonNode val = params.get(key);
                if (val != null && !val.asText().isEmpty()) {
                    return val.asText();
                }
            }
        }
        JsonNode metadata = controls.get("metadata");
        if (metadata != null && metadata.isArray()) {
            for (JsonNode item : metadata) {
                if (!item.isObject()) {
                    continue;
                }
                String key = item.path("key").asText().toLowerCase();
                if (key.equals("eagleeye-traceid") || key.equals("trace-id") || key.equals("x-trace-id")) {
                    JsonNode val = item.get("value");
                    if (val != null && !val.asText().isEmpty()) {
                        return val.asText();
                    }
                }
            }
        }
        return "";
    }

    private static List<Long> computeBlockKeys(List<Integer> tokenIds, int blockSize) {
        List<Long> keys = new ArrayList<>();
        int numBlocks = tokenIds.size() / blockSize;
        for (int b = 0; b < numBlocks; b++) {
            Hasher hasher = Hashing.murmur3_128().newHasher();
            for (int i = b * blockSize; i < (b + 1) * blockSize; i++) {
                hasher.putInt(tokenIds.get(i));
            }
            keys.add(hasher.hash().asLong());
        }
        return keys;
    }

    private static long stableRequestId(String value) {
        return Hashing.murmur3_128()
                .hashString(value, StandardCharsets.UTF_8)
                .asLong() & 0x7FFF_FFFF_FFFF_FFFFL;
    }

    private static long toSignedInt64(BigInteger value) {
        BigInteger mod = value.mod(BigInteger.ONE.shiftLeft(64));
        if (mod.compareTo(BigInteger.valueOf(Long.MAX_VALUE)) > 0) {
            mod = mod.subtract(BigInteger.ONE.shiftLeft(64));
        }
        return mod.longValue();
    }

    // ---- Server Latency HTTP ----

    private void resetServerLatency() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + config.targetAddr + "/rtp_llm/server_latency/reset"))
                    .POST(HttpRequest.BodyPublishers.noBody())
                    .timeout(Duration.ofSeconds(5))
                    .build();
            httpClient.send(request, HttpResponse.BodyHandlers.discarding());
        } catch (Exception e) {
            System.out.println("server latency reset unavailable: " + e.getMessage());
        }
    }

    private JsonNode fetchServerLatency() {
        try {
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create("http://" + config.targetAddr + "/rtp_llm/server_latency"))
                    .GET()
                    .timeout(Duration.ofSeconds(5))
                    .build();
            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            return MAPPER.readTree(response.body());
        } catch (Exception e) {
            System.out.println("server latency snapshot unavailable, using client RTT: " + e.getMessage());
            return MAPPER.createObjectNode();
        }
    }

    // ---- Output Writing ----

    private void writePerRequestResults() throws IOException {
        Path perRequestPath = Path.of(config.outputDir, "per_request.jsonl");
        try (BufferedWriter writer = Files.newBufferedWriter(perRequestPath)) {
            for (RequestResult result : results) {
                ObjectNode node = MAPPER.createObjectNode();
                node.put("rid", result.rid);
                node.put("trace_id", result.traceId);
                node.put("request_id", result.requestId);
                node.put("ts", result.ts);
                node.put("input_len", result.inputLen);
                node.put("output_len", result.outputLen);
                node.put("status", result.status);
                node.put("schedule_ms", result.scheduleMs);
                node.put("ttft_ms", result.ttftMs);
                node.put("total_ms", result.totalMs);
                node.put("enqueued_by_master", result.enqueuedByMaster);
                node.put("prefill", result.prefill);
                node.put("decode", result.decode);
                node.put("error", result.error);
                node.put("route_path", result.routePath);
                node.put("wall_clock_ts", result.wallClockTs);
                node.put("send_due_epoch_ms", result.sendDueEpochMs);
                node.put("send_start_epoch_ms", result.sendStartEpochMs);
                node.put("pacing_lag_ms", result.pacingLagMs);
                writer.write(node.toString());
                writer.newLine();
            }
        }
    }

    private void writeSummary(JsonNode serverLatency, double elapsedS, double sendDurationS, int sentCount)
            throws IOException {
        List<RequestResult> ok = results.stream().filter(r -> "ok".equals(r.status)).toList();
        List<RequestResult> scheduled = results.stream()
                .filter(r -> "ok".equals(r.status) || "scheduled".equals(r.status)).toList();
        int errorCount = results.size() - scheduled.size();
        int successCount = scheduled.size();

        List<Double> ttft = ok.stream().filter(r -> r.ttftMs > 0).map(r -> r.ttftMs).toList();
        List<Double> total = ok.stream().filter(r -> r.totalMs > 0).map(r -> r.totalMs).toList();
        List<Double> schedule = results.stream().filter(r -> r.scheduleMs > 0).map(r -> r.scheduleMs).toList();
        LatencySummary clientScheduleSummary = summarizeLatencies(schedule);

        JsonNode serverScheduleSummary = serverLatency.path("server_total_ms");
        boolean hasServerLatency = serverScheduleSummary.has("count") && serverScheduleSummary.get("count").asInt() > 0;

        ObjectNode serverStageLatency = MAPPER.createObjectNode();
        for (String stage : List.of("grpc_queue_ms", "route_submit_ms", "batch_wait_ms",
                "dispatch_ack_ms", "ack_response_ms")) {
            JsonNode stageNode = serverLatency.path(stage);
            serverStageLatency.set(stage, stageNode.isMissingNode() ? MAPPER.createObjectNode() : stageNode);
        }

        int slaViolations = (int) ok.stream().filter(r -> r.ttftMs > config.slaTtftMs).count();

        List<Double> sendStartTimes = new ArrayList<>();
        List<Double> pacingLags = new ArrayList<>();
        for (RequestResult r : results) {
            if (r.sendStartEpochMs > 0) {
                sendStartTimes.add(r.sendStartEpochMs);
                pacingLags.add(r.pacingLagMs);
            }
        }
        Collections.sort(sendStartTimes);
        double actualRpcQps = 0.0;
        if (sendStartTimes.size() > 1
                && sendStartTimes.get(sendStartTimes.size() - 1) > sendStartTimes.get(0)) {
            actualRpcQps = Math.round((sendStartTimes.size() - 1) * 1000.0
                    / (sendStartTimes.get(sendStartTimes.size() - 1) - sendStartTimes.get(0)) * 1000) / 1000.0;
        }

        ObjectNode summary = MAPPER.createObjectNode();
        summary.put("trace", config.traceFile);
        summary.put("max_concurrency", config.maxConcurrency);
        summary.put("elapsed_s", Math.round(elapsedS * 1000) / 1000.0);
        summary.put("total_requests", results.size());
        summary.put("scheduled", scheduled.size());
        summary.put("completed", ok.size());
        summary.put("errors", errorCount);
        summary.put("success_count", successCount);
        summary.put("error_count", errorCount);
        summary.put("offered_qps", elapsedS > 0 ? Math.round(results.size() / elapsedS * 1000) / 1000.0 : 0.0);
        summary.put("completed_qps", elapsedS > 0 ? Math.round(ok.size() / elapsedS * 1000) / 1000.0 : 0.0);
        summary.put("success_qps", elapsedS > 0 ? Math.round(successCount / elapsedS * 1000) / 1000.0 : 0.0);
        summary.put("error_qps", elapsedS > 0 ? Math.round(errorCount / elapsedS * 1000) / 1000.0 : 0.0);
        summary.put("send_duration_s", Math.round(sendDurationS * 1000) / 1000.0);
        summary.put("sent_count", sentCount);
        summary.put("actual_sent_count", actualSentCount.get());
        summary.put("recorded_result_count", results.size());
        summary.put("send_qps", sendDurationS > 0
                ? Math.round(results.size() / sendDurationS * 1000) / 1000.0 : 0.0);
        summary.put("actual_send_qps", actualRpcQps);
        summary.set("pacing_lag_ms", summarizeLatencies(pacingLags).toJson());

        ObjectNode peakQps = MAPPER.createObjectNode();
        for (int windowMs : List.of(1, 10, 100, 1000)) {
            peakQps.put(windowMs + "ms", peakBucketQps(sendStartTimes, windowMs));
        }
        summary.set("send_peak_qps", peakQps);
        summary.put("server_arrival_qps", serverLatency.path("arrival_qps").asDouble(0.0));
        summary.put("server_completion_qps", serverLatency.path("completion_qps").asDouble(0.0));
        summary.put("n_channels", config.nChannels);
        summary.put("sla_ttft_ms", config.slaTtftMs);
        summary.put("sla_violations", slaViolations);
        summary.put("sla_violation_rate", ok.isEmpty() ? 0.0
                : Math.round(slaViolations / (double) ok.size() * 1_000_000) / 1_000_000.0);
        summary.put("schedule_latency_source", hasServerLatency ? "server" : "client");
        summary.set("schedule_latency_ms", hasServerLatency ? serverScheduleSummary : clientScheduleSummary.toJson());
        summary.set("server_schedule_latency_ms", serverScheduleSummary.isMissingNode()
                ? MAPPER.createObjectNode() : serverScheduleSummary);
        summary.set("server_stage_latency_ms", serverStageLatency);
        summary.set("client_schedule_latency_ms", clientScheduleSummary.toJson());
        summary.set("ttft_ms", summarizeLatencies(ttft).toJson());
        summary.set("total_ms", summarizeLatencies(total).toJson());
        summary.set("prefill_balance", loadBalanceSummary(ok.stream().map(r -> r.prefill).toList()));
        summary.set("decode_balance", loadBalanceSummary(ok.stream().map(r -> r.decode).toList()));
        summary.set("status_counts", countBy(results, r -> r.status));
        summary.set("route_path_counts", countBy(results, r -> r.routePath));

        Path summaryPath = Path.of(config.outputDir, "summary.json");
        MAPPER.writerWithDefaultPrettyPrinter().writeValue(summaryPath.toFile(), summary);
        System.out.println(MAPPER.writerWithDefaultPrettyPrinter().writeValueAsString(summary));

        if (!serverLatency.isMissingNode() && !serverLatency.isEmpty()) {
            Path serverLatencyPath = Path.of(config.outputDir, "server_latency.json");
            MAPPER.writerWithDefaultPrettyPrinter().writeValue(serverLatencyPath.toFile(), serverLatency);
        }
    }

    // ---- Statistics Helpers ----

    private static LatencySummary summarizeLatencies(List<Double> values) {
        if (values.isEmpty()) {
            return new LatencySummary(0, 0, 0, 0, 0, 0, 0);
        }
        List<Double> sorted = new ArrayList<>(values);
        Collections.sort(sorted);
        double sum = 0;
        for (double v : sorted) {
            sum += v;
        }
        double mean = sum / sorted.size();
        return new LatencySummary(
                sorted.size(),
                percentile(sorted, 50),
                percentile(sorted, 90),
                percentile(sorted, 95),
                percentile(sorted, 99),
                sorted.get(sorted.size() - 1),
                Math.round(mean * 1000) / 1000.0
        );
    }

    private static double percentile(List<Double> sorted, double p) {
        if (sorted.isEmpty()) {
            return 0.0;
        }
        if (sorted.size() == 1) {
            return sorted.get(0);
        }
        double rank = (sorted.size() - 1) * p / 100.0;
        int lo = (int) Math.floor(rank);
        int hi = (int) Math.ceil(rank);
        if (lo == hi) {
            return sorted.get(lo);
        }
        double weight = rank - lo;
        return Math.round((sorted.get(lo) * (1.0 - weight) + sorted.get(hi) * weight) * 1000) / 1000.0;
    }

    private static double peakBucketQps(List<Double> epochMsValues, int windowMs) {
        if (epochMsValues.isEmpty() || windowMs <= 0) {
            return 0.0;
        }
        Map<Long, Integer> buckets = new HashMap<>();
        for (double value : epochMsValues) {
            long bucket = (long) (value / windowMs);
            buckets.merge(bucket, 1, Integer::sum);
        }
        int max = buckets.values().stream().max(Integer::compare).orElse(0);
        return Math.round(max * 1000.0 / windowMs * 1000) / 1000.0;
    }

    private static ObjectNode loadBalanceSummary(List<String> assignments) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (String addr : assignments) {
            if (addr != null && !addr.isEmpty()) {
                counts.merge(addr, 1, Integer::sum);
            }
        }
        ObjectNode node = MAPPER.createObjectNode();
        if (counts.isEmpty()) {
            node.set("counts", MAPPER.createObjectNode());
            node.put("stddev", 0.0);
            node.put("max_over_avg", 0.0);
            return node;
        }
        ObjectNode countsNode = MAPPER.createObjectNode();
        counts.forEach(countsNode::put);
        node.set("counts", countsNode);
        double avg = counts.values().stream().mapToInt(Integer::intValue).sum() / (double) counts.size();
        double variance = 0;
        for (int c : counts.values()) {
            variance += (c - avg) * (c - avg);
        }
        double stddev = Math.sqrt(variance / counts.size());
        int maxVal = counts.values().stream().max(Integer::compare).orElse(0);
        node.put("stddev", Math.round(stddev * 1000) / 1000.0);
        node.put("max_over_avg", avg > 0 ? Math.round(maxVal / avg * 1000) / 1000.0 : 0.0);
        return node;
    }

    private static ObjectNode countBy(List<RequestResult> rows, java.util.function.Function<RequestResult, String> extractor) {
        Map<String, Integer> counts = new LinkedHashMap<>();
        for (RequestResult row : rows) {
            String value = extractor.apply(row);
            counts.merge(value != null ? value : "", 1, Integer::sum);
        }
        ObjectNode node = MAPPER.createObjectNode();
        counts.forEach(node::put);
        return node;
    }

    // ---- Cleanup ----

    private void close() {
        for (ManagedChannel channel : scheduleChannels) {
            channel.shutdown();
        }
        for (ManagedChannel[] pool : engineChannelPools.values()) {
            for (ManagedChannel channel : pool) {
                channel.shutdown();
            }
        }
        eventLoopGroup.shutdownGracefully(0, 2, TimeUnit.SECONDS);
    }

    // ---- Inner Classes ----

    static final class Config {
        final String traceFile;
        final String targetAddr;
        final String grpcTarget;
        final int durationS;
        final int maxConcurrency;
        final double replaySpeed;
        final int loadClientWorkers;
        final String outputDir;
        final int numShards;
        final int shardIndex;
        final int limit;
        final long timeoutMs;
        final double slaTtftMs;
        final String zeroOutputPolicy;
        final boolean scheduleOnly;
        final boolean loop;
        final int nChannels;
        final int eventLoopThreads;
        final long startAtEpochMs;
        final int responseTimeoutSeconds;
        final boolean skipServerLatency;
        final String model;
        final String apiKey;
        final boolean fetchResponseEnabled;

        private Config(String traceFile, String targetAddr, String grpcTarget,
                       int durationS, int maxConcurrency, double replaySpeed,
                       int loadClientWorkers, String outputDir, int numShards,
                       int shardIndex, int limit, long timeoutMs, double slaTtftMs,
                       String zeroOutputPolicy, boolean scheduleOnly, boolean loop,
                       int nChannels, int eventLoopThreads, long startAtEpochMs,
                       int responseTimeoutSeconds, boolean skipServerLatency,
                       String model, String apiKey, boolean fetchResponseEnabled) {
            this.traceFile = traceFile;
            this.targetAddr = targetAddr;
            this.grpcTarget = grpcTarget;
            this.durationS = durationS;
            this.maxConcurrency = maxConcurrency;
            this.replaySpeed = replaySpeed;
            this.loadClientWorkers = loadClientWorkers;
            this.outputDir = outputDir;
            this.numShards = numShards;
            this.shardIndex = shardIndex;
            this.limit = limit;
            this.timeoutMs = timeoutMs;
            this.slaTtftMs = slaTtftMs;
            this.zeroOutputPolicy = zeroOutputPolicy;
            this.scheduleOnly = scheduleOnly;
            this.loop = loop;
            this.nChannels = nChannels;
            this.eventLoopThreads = eventLoopThreads;
            this.startAtEpochMs = startAtEpochMs;
            this.responseTimeoutSeconds = responseTimeoutSeconds;
            this.skipServerLatency = skipServerLatency;
            this.model = model;
            this.apiKey = apiKey;
            this.fetchResponseEnabled = fetchResponseEnabled;
        }

        static Config fromEnv() {
            String targetAddr = env("TARGET_ADDR", "127.0.0.1:7001");
            String grpcTarget = env("GRPC_TARGET", "");
            if (grpcTarget.isEmpty()) {
                int colon = targetAddr.lastIndexOf(':');
                String host = targetAddr.substring(0, colon);
                int httpPort = Integer.parseInt(targetAddr.substring(colon + 1));
                grpcTarget = host + ":" + (httpPort + 2);
            }
            boolean scheduleOnly = envBool("SCHEDULE_ONLY", false);
            String expectFetchResponse = env("FLEXLB_EXPECT_FETCH_RESPONSE", "");
            boolean fetchResponseEnabled = !scheduleOnly
                    && !expectFetchResponse.equalsIgnoreCase("0")
                    && !expectFetchResponse.equalsIgnoreCase("false")
                    && !expectFetchResponse.equalsIgnoreCase("no");
            return new Config(
                    env("TRACE_FILE", ""),
                    targetAddr,
                    grpcTarget,
                    envInt("DURATION_S", 0),
                    envInt("MAX_CONCURRENCY", 999_999_999),
                    envDouble("REPLAY_SPEED", 10.0),
                    envInt("LOAD_CLIENT_WORKERS", 1),
                    env("OUTPUT_DIR", "load_client_output"),
                    envInt("NUM_SHARDS", 1),
                    envInt("SHARD_INDEX", 0),
                    envInt("LIMIT", 0),
                    envLong("TIMEOUT_MS", 3_600_000L),
                    envDouble("SLA_TTFT_MS", 500.0),
                    env("ZERO_OUTPUT_POLICY", "skip"),
                    scheduleOnly,
                    envBool("LOOP", false),
                    envInt("N_CHANNELS", 8),
                    envInt("EVENT_LOOP_THREADS", 32),
                    envLong("START_AT_EPOCH_MS", 0L),
                    envInt("RESPONSE_TIMEOUT", 120),
                    envBool("SKIP_SERVER_LATENCY", false),
                    env("MODEL", "engine_service"),
                    env("API_KEY", ""),
                    fetchResponseEnabled
            );
        }

        void print() {
            System.out.println("=== JavaLoadClient Configuration ===");
            System.out.println("  TRACE_FILE=" + traceFile);
            System.out.println("  TARGET_ADDR=" + targetAddr);
            System.out.println("  GRPC_TARGET=" + grpcTarget);
            System.out.println("  DURATION_S=" + durationS);
            System.out.println("  MAX_CONCURRENCY=" + maxConcurrency);
            System.out.println("  REPLAY_SPEED=" + replaySpeed);
            System.out.println("  LOAD_CLIENT_WORKERS=" + loadClientWorkers);
            System.out.println("  OUTPUT_DIR=" + outputDir);
            System.out.println("  NUM_SHARDS=" + numShards);
            System.out.println("  SHARD_INDEX=" + shardIndex);
            System.out.println("  LIMIT=" + limit);
            System.out.println("  TIMEOUT_MS=" + timeoutMs);
            System.out.println("  SLA_TTFT_MS=" + slaTtftMs);
            System.out.println("  ZERO_OUTPUT_POLICY=" + zeroOutputPolicy);
            System.out.println("  SCHEDULE_ONLY=" + scheduleOnly);
            System.out.println("  LOOP=" + loop);
            System.out.println("  N_CHANNELS=" + nChannels);
            System.out.println("  EVENT_LOOP_THREADS=" + eventLoopThreads);
            System.out.println("  START_AT_EPOCH_MS=" + startAtEpochMs);
            System.out.println("  RESPONSE_TIMEOUT=" + responseTimeoutSeconds);
            System.out.println("  SKIP_SERVER_LATENCY=" + skipServerLatency);
            System.out.println("  MODEL=" + model);
            System.out.println("  FETCH_RESPONSE_ENABLED=" + fetchResponseEnabled);
            System.out.println("=====================================");
        }

        private static String env(String key, String def) {
            String val = System.getenv(key);
            return val != null && !val.isEmpty() ? val : def;
        }

        private static int envInt(String key, int def) {
            String val = System.getenv(key);
            return val != null && !val.isEmpty() ? Integer.parseInt(val) : def;
        }

        private static long envLong(String key, long def) {
            String val = System.getenv(key);
            return val != null && !val.isEmpty() ? Long.parseLong(val) : def;
        }

        private static double envDouble(String key, double def) {
            String val = System.getenv(key);
            return val != null && !val.isEmpty() ? Double.parseDouble(val) : def;
        }

        private static boolean envBool(String key, boolean def) {
            String val = System.getenv(key);
            if (val == null || val.isEmpty()) {
                return def;
            }
            return val.equals("1") || val.equalsIgnoreCase("true") || val.equalsIgnoreCase("yes");
        }
    }

    static final class TraceRecord {
        final long requestId;
        final String sourceRid;
        final String traceId;
        final long tsMs;
        final int inputLen;
        final int outputLen;
        final List<Long> blockKeys;
        final List<Integer> tokenIds;

        TraceRecord(long requestId, String sourceRid, String traceId, long tsMs,
                    int inputLen, int outputLen, List<Long> blockKeys, List<Integer> tokenIds) {
            this.requestId = requestId;
            this.sourceRid = sourceRid;
            this.traceId = traceId;
            this.tsMs = tsMs;
            this.inputLen = inputLen;
            this.outputLen = outputLen;
            this.blockKeys = blockKeys;
            this.tokenIds = tokenIds;
        }
    }

    static final class RequestResult {
        String rid = "";
        String traceId = "";
        long requestId;
        long ts;
        int inputLen;
        int outputLen;
        String status = "unknown";
        double scheduleMs;
        double ttftMs;
        double totalMs;
        boolean enqueuedByMaster;
        String prefill = "";
        String decode = "";
        String error = "";
        String routePath = "master";
        double wallClockTs;
        double sendDueEpochMs;
        double sendStartEpochMs;
        double pacingLagMs;
    }

    record LatencySummary(int count, double p50, double p90, double p95, double p99,
                          double max, double mean) {
        ObjectNode toJson() {
            ObjectNode node = MAPPER.createObjectNode();
            node.put("count", count);
            node.put("p50", p50);
            node.put("p90", p90);
            node.put("p95", p95);
            node.put("p99", p99);
            node.put("max", max);
            node.put("mean", mean);
            return node;
        }
    }
}
