package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.config.ConfigService;
import org.flexlb.engine.grpc.EngineGrpcClient;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

/**
 * Unified cleanup entry point for all batch-request termination scenarios.
 *
 * <p>Design principles:
 * <ol>
 *   <li>{@code tryTerminate()} CAS gate — only one thread executes cleanup.</li>
 *   <li>All cleanup actions are idempotent (CAS-guarded or self-protecting).</li>
 *   <li>{@code repackPrefillBatch()} is always called (skips if batchId &lt; 0) —
 *       this eliminates the historical {@code failAck} bug where repack was missing.</li>
 *   <li>{@code rollbackDecode()} is skipped when the future is already done
 *       (request succeeded) to avoid releasing decode KV while decode is running.</li>
 * </ol>
 */
@Component
public class CleanupCoordinator {

    private final EngineGrpcClient grpcClient;
    private final ConfigService configService;

    public CleanupCoordinator(EngineGrpcClient grpcClient, ConfigService configService) {
        this.grpcClient = grpcClient;
        this.configService = configService;
    }

    /**
     * Unified termination entry — all error/cancel/timeout scenarios.
     *
     * @param store  the inflight store to remove the item from
     * @param item   the batch item to terminate
     * @param reason categorises the termination scenario
     * @return true if this thread won the CAS gate and performed cleanup
     */
    public boolean terminate(BatchInflightStore store, BatchItem item, TerminationReason reason) {
        return terminate(store, item, reason, reason.defaultMessage());
    }

    /**
     * Unified termination entry with caller-supplied detail message.
     */
    public boolean terminate(BatchInflightStore store, BatchItem item, TerminationReason reason, String detail) {
        if (!item.tryTerminate()) {
            return false;  // CAS gate — another thread already terminated
        }

        store.remove(item.requestId());

        if (reason.isCancelLike()) {
            item.cancel();
        }

        // Don't rollback decode if the request already succeeded (prefill ack
        // received, future completed).  The decode is still running — releasing
        // KV prematurely would break it.  cancelOnEngine() will interrupt it.
        if (!item.future().isDone()) {
            item.rollbackDecode();
        }

        // Always call — self-guarding: no-op if batchId < 0 (not yet committed)
        item.repackPrefillBatch();

        if (reason.requiresEngineCancel()) {
            cancelOnEngine(item);
        }

        if (!item.future().isDone()) {
            item.future().complete(reason.toResponse(detail));
        }

        return true;
    }

    /**
     * Prefill error path — engine reported a failed prefill task.
     * <p>Removes from store and rolls back decode KV reservation.
     * Does NOT complete the future (the dispatch callback handles that).
     */
    public void onPrefillError(BatchInflightStore store, long requestId) {
        BatchItem item = store.remove(requestId);
        if (item != null) {
            item.rollbackDecode();
        }
    }

    /**
     * Decode completion path — engine reported a finished decode task.
     * <p>Only removes from store. No rollback needed (decode is done).
     */
    public void onDecodeComplete(BatchInflightStore store, long requestId) {
        store.remove(requestId);
    }

    /**
     * Cancel request on the prefill engine via gRPC.
     * Only prefill needs an explicit cancel — PrefillRpcServer::Cancel() cascades
     * internally to interrupt the prefill→decode flow.
     */
    private void cancelOnEngine(BatchItem item) {
        PrefillEndpoint prefillEp = item.prefillEp();
        if (prefillEp == null) {
            return;
        }
        try {
            long deadlineMs = configService.loadBalanceConfig().getFlexlbBatchEnqueueDeadlineMs();
            grpcClient.cancel(prefillEp.getIp(),
                    prefillEp.getGrpcPort(),
                    item.requestId(),
                    deadlineMs);
        } catch (RuntimeException e) {
            Logger.warn("FlexLB batch cancel failed for request {}", item.requestId(), e);
        }
    }
}
