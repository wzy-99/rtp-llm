package org.flexlb.balance.scheduler;

import org.flexlb.dao.loadbalance.Response;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

/**
 * Implements {@link DispatchCallback} — receives per-item gRPC dispatch results
 * from {@link BatchDispatcher} and completes request futures or delegates
 * failures to {@link CleanupCoordinator}.
 *
 * <p>Handles the <em>cancel-after-ack</em> race: if a cancel arrives between
 * dispatch and ack, the {@code synchronized} block ensures that either
 * (a) cancel wins and ack compensates with idempotent repack, or
 * (b) ack wins and completes the future before cancel can terminate.
 */
@Component
public class DispatchResultHandler implements DispatchCallback {

    private final BatchInflightStore store;
    private final CleanupCoordinator cleanupCoordinator;

    public DispatchResultHandler(BatchInflightStore store,
                                 CleanupCoordinator cleanupCoordinator) {
        this.store = store;
        this.cleanupCoordinator = cleanupCoordinator;
    }

    @Override
    public void onSuccess(BatchItem item, long batchId) {
        BatchItem entry = store.get(item.requestId());
        if (entry == null) {
            // cancel() already removed item and handled cleanup
            return;
        }

        boolean cancelAfterAck = false;
        synchronized (entry) {
            if (entry.isTerminated()) {
                // cancel won the race — terminate() already completed cleanup.
                // But batchId might not have been set yet (flushItems race).
                // Compensate: ensure repack (idempotent).
                entry.setBatchIdIfUnset(batchId);
                entry.repackPrefillBatch();
                return;
            }

            entry.setAckFinished(true);
            if (entry.isCancelled()) {
                // Cancel arrived between dispatch and ack.
                // The future is NOT done — terminate will handle cleanup.
                cancelAfterAck = true;
            } else if (!entry.future().isDone()) {
                Response success = BatchSchedulerUtils.copyResponse(item.routeResponse());
                success.setSuccess(true);
                success.setCode(200);
                success.setEnqueuedByMaster(true);
                success.setQueueLength(store.size());
                entry.future().complete(success);
                Logger.debug("FlexLB batch enqueued request {} in batch {}", item.requestId(), batchId);
            }
        }

        if (cancelAfterAck) {
            cleanupCoordinator.terminate(store, entry, TerminationReason.CANCELLED);
        }
    }

    @Override
    public void onFailure(BatchItem item, Throwable error) {
        cleanupCoordinator.terminate(store, item, TerminationReason.DISPATCH_FAILED, error.getMessage());
    }
}
