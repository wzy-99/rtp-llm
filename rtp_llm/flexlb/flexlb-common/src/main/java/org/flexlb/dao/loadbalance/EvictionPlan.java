package org.flexlb.dao.loadbalance;

import java.util.List;

/**
 * Immutable eviction plan produced by the v4 cost-based eviction planner.
 *
 * <p>Aggregates the victims selected for eviction on a single endpoint, the
 * plan's total cost (computed via {@link org.flexlb.balance.cost.EvictionCostFunction}),
 * and whether the plan is feasible (all-or-nothing check: the freed resources
 * must satisfy the incoming request's deficit).
 *
 * <p>Uses {@link PlanVictim} instead of {@code EvictionVictim} (flexlb-sync)
 * because this class resides in flexlb-common, which does not depend on
 * flexlb-sync. The strategy layer converts {@code EvictionVictim} to
 * {@link PlanVictim} when building plans.
 *
 * @param endpointIpPort the target endpoint's ip:port
 * @param caseType        rejection reason that triggered eviction (RESOURCE_UNAVAILABLE / COMPUTE_SATURATED / KV_CAPACITY / KV_UNAVAILABLE)
 * @param victims         victims to evict
 * @param cost            plan cost (lower = cheaper to evict)
 * @param feasible        all-or-nothing check passed (freed resources >= deficit)
 */
public record EvictionPlan(
        String endpointIpPort,
        RejectionReason caseType,
        List<PlanVictim> victims,
        double cost,
        boolean feasible
) {}
