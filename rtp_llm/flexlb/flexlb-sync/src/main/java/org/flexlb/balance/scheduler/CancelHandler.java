package org.flexlb.balance.scheduler;

import org.flexlb.schedule.grpc.FlexlbScheduleProtocol.CancelReasonPB;

/**
 * Functional interface for cancelling an inflight request via the cancel RPC chain.
 *
 * <p>Implemented by {@link org.flexlb.service.RouteService} and injected into
 * {@link DefaultRouter} via {@code ObjectProvider} to avoid circular dependency.
 * Phase 4 uses a stub implementation; Phase 5 wires the actual cancel RPC.
 */
@FunctionalInterface
public interface CancelHandler {
    /**
     * Cancel an inflight request with the given reason.
     *
     * @param requestId the request to cancel
     * @param reason    the cancel reason (e.g. {@code CANCEL_REASON_PRIORITY_PREEMPTED})
     */
    void cancel(long requestId, CancelReasonPB reason);
}
