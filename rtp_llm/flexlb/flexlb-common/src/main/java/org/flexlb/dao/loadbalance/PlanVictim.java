package org.flexlb.dao.loadbalance;

import org.flexlb.enums.TaskPhase;

/**
 * Immutable snapshot of a single eviction victim, used by the v4 cost-based
 * eviction planner in {@link EvictionPlan}.
 *
 * <p>This is the flexlb-common counterpart of {@code EvictionVictim} in
 * flexlb-sync. The split exists because {@code EvictionVictim} lives in
 * flexlb-sync (which depends on flexlb-common), while {@link EvictionPlan}
 * must reside in flexlb-common. The strategy layer converts between the two
 * when building plans.
 *
 * @param requestId  the victim's request ID
 * @param kvTokens   hard KV tokens occupied by the victim (0 for prefill queue victims)
 * @param priority   victim's priority level (strictly less than incoming)
 * @param taskPhase  victim's engine-side phase (null for prefill queue victims)
 */
public record PlanVictim(
        long requestId,
        long kvTokens,
        int priority,
        TaskPhase taskPhase
) {}
