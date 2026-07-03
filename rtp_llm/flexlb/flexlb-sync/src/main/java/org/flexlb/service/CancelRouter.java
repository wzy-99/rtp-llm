package org.flexlb.service;

import org.flexlb.balance.scheduler.BatchScheduler;
import org.flexlb.balance.scheduler.QueueManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

/**
 * Unified cancel entry point for all three routing paths.
 *
 * <ol>
 *   <li>Batch path — delegates to {@link BatchScheduler#cancel(long)} which
 *       uses {@link org.flexlb.balance.scheduler.CleanupCoordinator}</li>
 *   <li>Queue path — delegates to {@link QueueManager#cancelByRequestId(long)}</li>
 *   <li>Direct path — no inflight state, nothing to cancel</li>
 * </ol>
 */
@Component
public class CancelRouter {

    private final BatchScheduler batchScheduler;
    private final QueueManager queueManager;

    public CancelRouter(@Lazy @Autowired(required = false) BatchScheduler batchScheduler,
                        QueueManager queueManager) {
        this.batchScheduler = batchScheduler;
        this.queueManager = queueManager;
    }

    /**
     * Cancel a request by ID across all routing paths.
     *
     * @param requestId the request ID to cancel
     * @return true if the request was found and cancelled in any path
     */
    public boolean cancel(long requestId) {
        // 1. Try Batch path
        if (batchScheduler != null && batchScheduler.cancel(requestId)) {
            return true;
        }
        // 2. Try Queue path
        if (queueManager != null && queueManager.cancelByRequestId(requestId)) {
            return true;
        }
        // 3. Direct path: no inflight state, nothing to cancel
        return false;
    }
}
