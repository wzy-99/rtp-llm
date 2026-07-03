package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Logger;
import org.springframework.context.annotation.Lazy;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * Batch-path orchestrator — the slimmed-down successor to the former
 * {@code FlexlbBatchScheduler} God Class.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>{@link #submit} — admission control, routing, BatchItem creation, offer to batcher</li>
 *   <li>{@link #cancel} — lookup in store and delegate to {@link CleanupCoordinator}</li>
 *   <li>{@link #onWorkerStatusUpdate} — natural completion paths (prefill error, decode done)</li>
 *   <li>{@link #cleanupInflight} — scheduled TTL eviction</li>
 * </ul>
 *
 * <p>Batch assembly and gRPC dispatch are handled by {@link BatchDispatchCoordinator}.
 * Per-item dispatch results are handled by {@link DispatchResultHandler}.
 * Metrics reporting is handled by {@link BatchMetricsCollector}.
 */
@Component
public class BatchScheduler {

    private final ConfigService configService;
    private final Router router;
    private final EndpointRegistry endpointRegistry;
    private final BatchInflightStore store;
    private final CleanupCoordinator cleanupCoordinator;

    public BatchScheduler(ConfigService configService,
                          @Lazy Router router,
                          EndpointRegistry endpointRegistry,
                          BatchInflightStore store,
                          CleanupCoordinator cleanupCoordinator) {
        this.configService = configService;
        this.router = router;
        this.endpointRegistry = endpointRegistry;
        this.store = store;
        this.cleanupCoordinator = cleanupCoordinator;
    }

    // ==================== Request submission ====================

    public CompletableFuture<Response> submit(FlexlbRequest request) {
        try {
            if (request == null || request.getRequest() == null) {
                request.getFuture().complete(Response.error(StrategyErrorType.INVALID_REQUEST));
                return request.getFuture();
            }

            int maxInflight = configService.loadBalanceConfig().getFlexlbBatchMaxInflight();
            if (maxInflight > 0 && store.size() >= maxInflight) {
                Response resp = Response.error(StrategyErrorType.QUEUE_FULL);
                request.getFuture().complete(resp);
                return request.getFuture();
            }

            Response routeResponse = router.route(request);
            if (routeResponse == null || !routeResponse.isSuccess()) {
                request.getFuture().complete(routeResponse != null
                        ? routeResponse
                        : Response.error(StrategyErrorType.NO_AVAILABLE_WORKER));
                return request.getFuture();
            }

            ServerStatus prefill = BatchSchedulerUtils.findServer(routeResponse, RoleType.PREFILL);
            ServerStatus decode = BatchSchedulerUtils.findServer(routeResponse, RoleType.DECODE);
            if (prefill == null) {
                rollback(routeResponse);
                Response resp = Response.error(StrategyErrorType.NO_PREFILL_WORKER);
                request.getFuture().complete(resp);
                return request.getFuture();
            }

            String prefillIpPort = prefill.getServerIp() + ":" + prefill.getHttpPort();
            PrefillEndpoint prefillEp = endpointRegistry.getPrefill(prefillIpPort);
            if (prefillEp == null) {
                rollback(routeResponse);
                Response resp = Response.error(StrategyErrorType.NO_PREFILL_WORKER);
                request.getFuture().complete(resp);
                return request.getFuture();
            }

            DecodeEndpoint decodeEp = null;
            if (decode != null) {
                String decodeIpPort = decode.getServerIp() + ":" + decode.getHttpPort();
                decodeEp = endpointRegistry.getDecode(decodeIpPort);
            }

            BatchItem item = new BatchItem(request, routeResponse, BatchSchedulerUtils.copyOf(prefill),
                    BatchSchedulerUtils.copyOf(decode),
                    prefillEp, decodeEp, /* sortKey set by batcher */ 0, System.currentTimeMillis());
            store.put(request.getRequestId(), item);
            WorkerBatcher batcher = prefillEp.getBatcher();
            batcher.offer(item);
        } catch (Throwable t) {
            if (request != null) {
                store.remove(request.getRequestId());
            }
            Logger.error("BatchScheduler submit failed for request id: {}",
                    request == null ? null : request.getRequestId(), t);
            Response errorResp = new Response();
            errorResp.setSuccess(false);
            errorResp.setCode(StrategyErrorType.BATCH_DISPATCH_FAILED.getErrorCode());
            errorResp.setErrorMessage("Submit failed: " + t.getMessage());
            request.getFuture().complete(errorResp);
        }
        return request.getFuture();
    }

    // ==================== Cancellation ====================

    public boolean cancel(long requestId) {
        BatchItem item = store.get(requestId);
        if (item == null) {
            Logger.debug("flexlb batch cancel ignored; request {} not found in inflight", requestId);
            return false;
        }
        return cleanupCoordinator.terminate(store, item, TerminationReason.CANCELLED);
    }

    // ==================== Completion from worker status ====================

    public void onWorkerStatusUpdate(WorkerStatus ws, WorkerStatusResponse response) {
        if (response == null) {
            return;
        }
        Map<String, TaskInfo> finishedTaskInfo = response.getFinishedTaskInfo();
        if (finishedTaskInfo == null || finishedTaskInfo.isEmpty()) {
            return;
        }

        boolean isPrefill = response.getRole() == RoleType.PREFILL;

        for (TaskInfo task : finishedTaskInfo.values()) {
            long requestId = task.getRequestId();

            // Prefill success: decode is still running, keep scheduler inflight entry
            if (isPrefill && task.getErrorCode() == 0) {
                continue;
            }

            // Prefill error: rollback decode KV reservation since decode will never run
            if (isPrefill) {
                cleanupCoordinator.onPrefillError(store, requestId);
            } else {
                // Decode completion (success or error): scheduler only cleans its own map.
                // DecodeEndpoint.calibrate() independently handles its own inflightRequests cleanup.
                cleanupCoordinator.onDecodeComplete(store, requestId);
            }
        }
    }

    // ==================== Inflight TTL cleanup ====================

    @Scheduled(fixedRate = 60000L)
    public void cleanupInflight() {
        long ttlMs = configService.loadBalanceConfig().getFlexlbInflightTtlMs();
        store.forEachEvictable(ttlMs, item ->
            cleanupCoordinator.terminate(store, item, TerminationReason.TTL_EXPIRED));
    }

    // ==================== Internal: resource rollback (submit early-return) ====================

    /**
     * Rollback using route response — used only in submit() early-return paths
     * where BatchItem has not been created yet.
     */
    private void rollback(Response routeResponse) {
        if (routeResponse == null || routeResponse.getServerStatus() == null) {
            return;
        }
        for (ServerStatus serverStatus : routeResponse.getServerStatus()) {
            rollback(serverStatus);
        }
    }

    private void rollback(ServerStatus serverStatus) {
        if (serverStatus == null) {
            return;
        }
        if (serverStatus.getRole() == RoleType.DECODE) {
            String ipPort = serverStatus.getServerIp() + ":" + serverStatus.getHttpPort();
            DecodeEndpoint ep = endpointRegistry.getDecode(ipPort);
            if (ep != null) {
                ep.release(serverStatus.getRequestId());
            }
        }
    }

    // ==================== Lifecycle ====================

    @PreDestroy
    public void shutdown() {
        endpointRegistry.close();
    }
}
