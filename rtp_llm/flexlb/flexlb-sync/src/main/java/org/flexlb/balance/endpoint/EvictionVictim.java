package org.flexlb.balance.endpoint;

import org.flexlb.enums.TaskPhase;

/**
 * Immutable snapshot of a single eviction victim, returned by the
 * {@code findEvictable*} methods on {@link DecodeEndpoint} and
 * {@link PrefillEndpoint}.
 *
 * <p>Carries enough information for the caller (DefaultRouter) to:
 * <ul>
 *   <li>Sum freed KV tokens (for KV_FULL path)</li>
 *   <li>Identify the victim for engine cancel (requestId)</li>
 *   <li>Log the victim's priority / phase for observability</li>
 * </ul>
 *
 * @param requestId  the victim's request ID
 * @param kvTokens   hard KV tokens occupied by the victim (0 for prefill queue victims)
 * @param priority   victim's priority level (strictly less than incoming)
 * @param taskPhase  victim's engine-side phase (null for prefill queue victims)
 */
public record EvictionVictim(
        long requestId,
        long kvTokens,
        int priority,
        TaskPhase taskPhase
) {}
