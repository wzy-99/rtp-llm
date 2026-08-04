package org.flexlb.balance.cost;

import org.flexlb.dao.loadbalance.RejectionReason;

/**
 * Cost function interface for the v4 cost-based eviction planner.
 *
 * <p>Each method maps a single dimension of the eviction cost formula to a
 * weight. The total eviction cost of a plan is the weighted combination of
 * these factors, allowing the planner to select the cheapest eviction plan
 * that satisfies the resource deficit.
 *
 * <ul>
 *   <li>{@link #f(int)} — priority &rarr; eviction cost weight (higher priority = more expensive to evict)</li>
 *   <li>{@link #g(int)} — run status &rarr; eviction cost weight (running = most expensive)</li>
 *   <li>{@link #j(long)} — KV tokens released &rarr; eviction advantage (higher = better)</li>
 *   <li>{@link #k(long)} — KV token length &rarr; eviction cost (inverse of j)</li>
 *   <li>{@link #h(RejectionReason)} — case type &rarr; scenario weight</li>
 * </ul>
 */
public interface EvictionCostFunction {

    /**
     * Priority &rarr; eviction cost weight.
     * Higher priority requests are more expensive to evict.
     *
     * @param priority the victim's priority level
     * @return eviction cost weight
     */
    double f(int priority);

    /**
     * Run status &rarr; eviction cost weight.
     * Uses ordinal to avoid import dependencies on the run-status enum.
     * <ul>
     *   <li>0 = NOT_ACCEPTED (cheapest)</li>
     *   <li>1 = ACCEPTED_NOT_RUNNING</li>
     *   <li>2 = RUNNING (most expensive)</li>
     * </ul>
     *
     * @param runStatusOrdinal the run-status ordinal
     * @return eviction cost weight
     */
    double g(int runStatusOrdinal);

    /**
     * KV tokens released &rarr; eviction advantage.
     * Higher released KV tokens mean the eviction is more beneficial.
     *
     * @param kvTokens the KV tokens released by eviction
     * @return eviction advantage weight
     */
    double j(long kvTokens);

    /**
     * KV token length &rarr; eviction cost (inverse of {@link #j}).
     * Longer sequences are more costly to evict (wasted compute).
     *
     * @param kvTokens the victim's KV token length
     * @return eviction cost weight
     */
    double k(long kvTokens);

    /**
     * Case type &rarr; scenario weight.
     * Different rejection reasons may warrant different eviction priorities.
     *
     * @param caseType the rejection reason that triggered eviction
     * @return scenario weight
     */
    double h(RejectionReason caseType);
}
