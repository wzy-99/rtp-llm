package org.flexlb.dao.loadbalance;

/**
 * Immutable snapshot of a single endpoint's feasibility assessment produced
 * during the filter stage of a cost-based eviction selection.
 *
 * <p>Carries the rejection reason plus the quantitative deficit that caused
 * the rejection. Consumed by {@link ServerStatus#failureWithFeasibility} to
 * build a structured rejection report for the eviction planner.
 *
 * @param ipPort       endpoint identifier (ip:port)
 * @param reason       rejection reason
 * @param kvDeficit    KV token deficit (0 if not KV-related)
 * @param slotDeficit  decode concurrency slot deficit (0 if not concurrency-related)
 * @param queueDeficit prefill queue overflow count (0 if not queue-related)
 */
public record EndpointFeasibility(
        String ipPort,
        RejectionReason reason,
        long kvDeficit,
        long slotDeficit,
        int queueDeficit
) {}
