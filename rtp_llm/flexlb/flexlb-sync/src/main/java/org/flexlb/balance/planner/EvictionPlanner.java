package org.flexlb.balance.planner;

import org.flexlb.balance.cost.DefaultEvictionCostFunction;
import org.flexlb.balance.cost.EvictionCostFunction;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EvictionVictim;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.scheduler.BatchItem;
import org.flexlb.balance.scheduler.CancelHandler;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.EndpointFeasibility;
import org.flexlb.dao.loadbalance.EvictionPlan;
import org.flexlb.dao.loadbalance.PlanVictim;
import org.flexlb.dao.loadbalance.RejectionReason;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.flexlb.schedule.grpc.FlexlbScheduleProtocol.CancelReasonPB;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.sync.status.EngineWorkerStatus;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * V4 cost-based eviction planner.
 *
 * <p>Core component of the v4 design. When the strategy {@code select()} fails,
 * it returns {@code ServerStatus.failureWithFeasibility(code, List<EndpointFeasibility>)}.
 * The EvictionPlanner then:
 * <ol>
 *   <li>{@link #planAll} — generates eviction plans for each rejected endpoint
 *       based on its rejection reason, using the cost function to rank victims.</li>
 *   <li>{@link #selectBest} — selects the minimum-cost feasible plan across
 *       all endpoints.</li>
 *   <li>{@link #commit} — executes the selected plan: evicts victims and frees
 *       resources on the target endpoint.</li>
 * </ol>
 *
 * <p>Four eviction cases are handled:
 * <ul>
 *   <li>Case 1: Prefill queue full (RESOURCE_UNAVAILABLE) — evict queued BatchItems.</li>
 *   <li>Case 2: Decode slot full (COMPUTE_SATURATED) — evict concurrency victims.</li>
 *   <li>Case 3: Decode KV not enough (KV_CAPACITY / KV_UNAVAILABLE) — evict KV victims.</li>
 *   <li>Case 4: Both slot and KV full — combined eviction with slot-first check
 *       to find single-dimension plans that cover both deficits.</li>
 * </ul>
 */
@Component
public class EvictionPlanner {

    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger("syncLogger");

    private final EvictionCostFunction costFunction;
    private final EngineWorkerStatus engineWorkerStatus;
    private final ObjectProvider<CancelHandler> cancelHandlerProvider;
    private final ObjectProvider<BatchSchedulerReporter> reporterProvider;

    /**
     * Build the planner with a {@link DefaultEvictionCostFunction} configured from
     * {@link FlexlbConfig} v4 parameters.
     */
    public EvictionPlanner(ConfigService configService,
                           EngineWorkerStatus engineWorkerStatus,
                           ObjectProvider<CancelHandler> cancelHandlerProvider,
                           ObjectProvider<BatchSchedulerReporter> reporterProvider) {
        FlexlbConfig config = configService.loadBalanceConfig();
        this.costFunction = new DefaultEvictionCostFunction(
                config.getV4CostM(),
                config.getV4GNotAccepted(),
                config.getV4GAcceptedNotRunning(),
                config.getV4GRunning(),
                config.getV4HSlot(),
                config.getV4HKv(),
                config.getV4MaxVictimsPerDecision(),
                parsePriorityLevels(config.getFlexlbPriorityLevels())
        );
        this.engineWorkerStatus = engineWorkerStatus;
        this.cancelHandlerProvider = cancelHandlerProvider;
        this.reporterProvider = reporterProvider;
    }

    /**
     * Parse a comma-separated priority-levels string (e.g. "30,40,50,60,70")
     * into a sorted int array for use by {@link DefaultEvictionCostFunction}.
     */
    private static int[] parsePriorityLevels(String levels) {
        if (levels == null || levels.isBlank()) {
            return new int[]{30, 40, 50, 60, 70};
        }
        String[] parts = levels.split(",");
        int[] result = new int[parts.length];
        for (int i = 0; i < parts.length; i++) {
            try {
                result[i] = Integer.parseInt(parts[i].trim());
            } catch (NumberFormatException e) {
                result[i] = 30 + i * 10; // fallback
            }
        }
        java.util.Arrays.sort(result);
        return result;
    }

    // ==================== planAll ====================

    /**
     * Generate eviction plans for all endpoints described in the feasibilities list.
     *
     * <p>For each {@link EndpointFeasibility}, dispatch to the appropriate case based
     * on {@link EndpointFeasibility#reason()}, generate a plan with victim list,
     * cost, and all-or-nothing feasibility.
     *
     * @param feasibilities per-endpoint feasibility assessments from the strategy
     * @param ctx           the incoming request's balance context
     * @param roleType      the role being routed (PREFILL / DECODE)
     * @param group         the routing group (may be null for no group filter)
     * @return list of plans (may include infeasible plans; use {@link #selectBest} to filter)
     */
    public List<EvictionPlan> planAll(List<EndpointFeasibility> feasibilities,
                                      BalanceContext ctx, RoleType roleType, String group) {
        int incomingPriority = ctx.getPriority();
        Map<String, WorkerEndpoint> endpoints = engineWorkerStatus.selectModelWorkerStatus(roleType, group);

        List<EvictionPlan> plans = new ArrayList<>();
        for (EndpointFeasibility ef : feasibilities) {
            WorkerEndpoint ep = endpoints.get(ef.ipPort());
            if (ep == null || !ep.getStatus().isAlive()) {
                continue;
            }
            EvictionPlan plan = planForEndpoint(ef, ep, incomingPriority);
            if (plan != null) {
                plans.add(plan);
            }
        }
        return plans;
    }

    /**
     * Dispatch a single endpoint to the appropriate case based on its rejection reason.
     * Case 4 (combined slot + KV deficit) is detected first via both deficit fields
     * being populated, before the reason-based switch.
     */
    private EvictionPlan planForEndpoint(EndpointFeasibility ef, WorkerEndpoint ep, int incomingPriority) {
        // Case 4: both slot and KV deficit — dispatched before reason-based switch
        // so that simultaneous deficits (populated by the strategy) reach planCombined.
        if (ef.slotDeficit() > 0 && ef.kvDeficit() > 0 && ep instanceof DecodeEndpoint de) {
            return planCombined(ef, de, incomingPriority);
        }

        return switch (ef.reason()) {
            case RESOURCE_UNAVAILABLE -> ep instanceof PrefillEndpoint pe
                    ? planPrefillQueue(ef, pe, incomingPriority) : null;
            case COMPUTE_SATURATED -> ep instanceof DecodeEndpoint de
                    ? planDecodeSlot(ef, de, incomingPriority) : null;
            case KV_CAPACITY, KV_UNAVAILABLE -> ep instanceof DecodeEndpoint de
                    ? planDecodeKv(ef, de, incomingPriority) : null;
            default -> null;
        };
    }

    // ==================== Case 1: Prefill queue full (RESOURCE_UNAVAILABLE) ====================

    /**
     * Case 1: Prefill batcher PQ eviction.
     *
     * <p>Use {@link PrefillEndpoint#findEvictableQueuedRequests} to get candidates,
     * sort by f(priority) ascending then sortKey descending (more SLO slack = more evictable),
     * greedily select until victims.size() >= queueDeficit.
     *
     * <p>Cost = Σ f(priority) for each victim.
     * All-or-nothing: if victims.size() < queueDeficit → infeasible.
     */
    private EvictionPlan planPrefillQueue(EndpointFeasibility ef, PrefillEndpoint pe, int incomingPriority) {
        int queueDeficit = ef.queueDeficit();
        if (queueDeficit <= 0) {
            return new EvictionPlan(ef.ipPort(), ef.reason(), List.of(), 0.0, true);
        }

        List<BatchItem> items = pe.findEvictableQueuedRequests(incomingPriority, queueDeficit);

        // Sort: f(priority) ascending, then sortKey descending (more SLO slack = more evictable)
        List<BatchItem> sorted = new ArrayList<>(items);
        sorted.sort(Comparator
                .comparingDouble((BatchItem i) -> costFunction.f(i.priority()))
                .thenComparing(BatchItem::sortKey, Comparator.reverseOrder()));

        // Greedy: take items until victims.size() >= queueDeficit
        List<BatchItem> selected = sorted.size() > queueDeficit
                ? new ArrayList<>(sorted.subList(0, queueDeficit)) : sorted;

        boolean feasible = selected.size() >= queueDeficit;
        double cost = 0;
        List<PlanVictim> victims = new ArrayList<>();
        for (BatchItem item : selected) {
            cost += costFunction.f(item.priority());
            victims.add(new PlanVictim(item.requestId(), 0, item.priority(), null));
        }

        Logger.info("EvictionPlanner Case 1 (prefill queue): ep={}, victims={}, queueDeficit={}, cost={}, feasible={}",
                ef.ipPort(), victims.size(), queueDeficit, cost, feasible);
        return new EvictionPlan(ef.ipPort(), ef.reason(), victims, cost, feasible);
    }

    // ==================== Case 2: Decode slot full (COMPUTE_SATURATED) ====================

    /**
     * Case 2: Decode concurrency slot eviction.
     *
     * <p>Two-pass: pass1 tries unconfirmed concurrency victim (cheapest),
     * pass2 tries running concurrency victim. Sort by f(priority) ascending
     * then g(runStatus) ascending (NOT_ACCEPTED before RUNNING).
     *
     * <p>Cost = Σ [f(priority) × g(runStatus)] × h(COMPUTE_SATURATED).
     * All-or-nothing: if can't free enough slots → infeasible.
     */
    private EvictionPlan planDecodeSlot(EndpointFeasibility ef, DecodeEndpoint de, int incomingPriority) {
        int slotDeficit = (int) ef.slotDeficit();
        if (slotDeficit <= 0) {
            return new EvictionPlan(ef.ipPort(), ef.reason(), List.of(), 0.0, true);
        }

        List<EvictionVictim> victims = new ArrayList<>();

        // Pass 1: unconfirmed
        EvictionVictim v1 = de.findUnconfirmedEvictableConcurrencyVictim(incomingPriority);
        if (v1 != null) {
            victims.add(v1);
        }

        // Pass 2: running (if still need more slots)
        if (victims.size() < slotDeficit) {
            EvictionVictim v2 = de.findRunningEvictableConcurrencyVictim(incomingPriority);
            if (v2 != null) {
                victims.add(v2);
            }
        }

        // Sort: f(priority) ascending, then g(runStatus) ascending (NOT_ACCEPTED before RUNNING)
        victims.sort(Comparator
                .comparingDouble((EvictionVictim v) -> costFunction.f(v.priority()))
                .thenComparingDouble(v -> costFunction.g(taskPhaseToGOrdinal(v.taskPhase()))));

        // Greedy: until slotDeficit victims freed
        List<EvictionVictim> selected = victims.size() > slotDeficit
                ? new ArrayList<>(victims.subList(0, slotDeficit)) : victims;

        boolean feasible = selected.size() >= slotDeficit;
        double cost = 0;
        List<PlanVictim> planVictims = new ArrayList<>();
        for (EvictionVictim v : selected) {
            int ordinal = taskPhaseToGOrdinal(v.taskPhase());
            cost += costFunction.f(v.priority()) * costFunction.g(ordinal);
            planVictims.add(toPlanVictim(v));
        }
        cost *= costFunction.h(RejectionReason.COMPUTE_SATURATED);

        Logger.info("EvictionPlanner Case 2 (decode slot): ep={}, victims={}, slotDeficit={}, cost={}, feasible={}",
                ef.ipPort(), planVictims.size(), slotDeficit, cost, feasible);
        return new EvictionPlan(ef.ipPort(), ef.reason(), planVictims, cost, feasible);
    }

    // ==================== Case 3: Decode KV not enough (KV_CAPACITY / KV_UNAVAILABLE) ====================

    /**
     * Case 3: Decode KV token eviction.
     *
     * <p>Two-pass: pass1 tries unconfirmed KV victims, pass2 tries running KV victims
     * for the remaining deficit. Sort by f(priority) ascending, then kvBucket descending
     * (j(kvReleased) — more KV released per victim first), then g(runStage) ascending
     * (earlier stage first), then requestId descending (deterministic tiebreaker).
     * Greedily select until Σ(kvReleased) >= kvDeficit.
     *
     * <p>Cost = Σ [f(priority) × k(length) × g(runStatus)] × h(reason).
     * All-or-nothing: if Σ(kvReleased) < kvDeficit → infeasible.
     */
    private EvictionPlan planDecodeKv(EndpointFeasibility ef, DecodeEndpoint de, int incomingPriority) {
        long kvDeficit = ef.kvDeficit();
        if (kvDeficit <= 0) {
            return new EvictionPlan(ef.ipPort(), ef.reason(), List.of(), 0.0, true);
        }

        List<EvictionVictim> victims = new ArrayList<>();

        // Pass 1: unconfirmed
        List<EvictionVictim> list1 = de.findUnconfirmedEvictableVictims(incomingPriority, kvDeficit);
        victims.addAll(list1);
        long freed = victims.stream().mapToLong(EvictionVictim::kvTokens).sum();

        // Pass 2: running (if still need more KV)
        if (freed < kvDeficit) {
            long remaining = kvDeficit - freed;
            List<EvictionVictim> list2 = de.findRunningEvictableVictims(incomingPriority, remaining);
            victims.addAll(list2);
        }

        // Sort: f(priority) ascending, then kvBucket descending (j(kvReleased) — more KV per victim first),
        // then g(runStage) ascending (earlier stage first), then requestId descending (deterministic tiebreaker)
        victims.sort(Comparator
                .comparingDouble((EvictionVictim v) -> costFunction.f(v.priority()))
                .thenComparingDouble(v -> -costFunction.j(v.kvTokens()))
                .thenComparingDouble(v -> costFunction.g(taskPhaseToGOrdinal(v.taskPhase())))
                .thenComparing(EvictionVictim::requestId, Comparator.reverseOrder()));

        // Greedy: until Σ(kvReleased) >= kvDeficit
        List<EvictionVictim> selected = new ArrayList<>();
        long accumulated = 0;
        for (EvictionVictim v : victims) {
            selected.add(v);
            accumulated += v.kvTokens();
            if (accumulated >= kvDeficit) {
                break;
            }
        }

        boolean feasible = accumulated >= kvDeficit;
        double cost = 0;
        List<PlanVictim> planVictims = new ArrayList<>();
        for (EvictionVictim v : selected) {
            int ordinal = taskPhaseToGOrdinal(v.taskPhase());
            cost += costFunction.f(v.priority())
                    * costFunction.k(v.kvTokens())
                    * costFunction.g(ordinal);
            planVictims.add(toPlanVictim(v));
        }
        cost *= costFunction.h(ef.reason());

        Logger.info("EvictionPlanner Case 3 (decode KV): ep={}, victims={}, kvDeficit={}, accumulated={}, cost={}, feasible={}",
                ef.ipPort(), planVictims.size(), kvDeficit, accumulated, cost, feasible);
        return new EvictionPlan(ef.ipPort(), ef.reason(), planVictims, cost, feasible);
    }

    // ==================== Case 4: Both slot and KV full ====================

    /**
     * Case 4: Combined KV + slot eviction.
     *
     * <p>When both slot and KV deficits are populated (produced by the strategy's
     * simultaneous deficit capture), try two single-dimension plans first:
     * <ol>
     *   <li>KV-only plan (Case 3): check if the freed slots also cover slotDeficit.</li>
     *   <li>Slot-only plan (Case 2): check if the released KV also covers kvDeficit.</li>
     * </ol>
     * If either single-dimension plan covers both deficits, use it (picking the
     * cheaper one when both qualify). Otherwise fall back to the combined approach:
     * KV eviction first, then slot eviction for remaining slots.
     *
     * <p>Cost = KV cost (from Case 3) + slot cost (from Case 2) for remaining.
     */
    private EvictionPlan planCombined(EndpointFeasibility ef, DecodeEndpoint de, int incomingPriority) {
        int slotDeficit = (int) ef.slotDeficit();
        long kvDeficit = ef.kvDeficit();

        // 1. Try KV-only plan (Case 3)
        EvictionPlan kvPlan = planDecodeKv(ef, de, incomingPriority);
        int slotsFromKv = kvPlan.victims().size();
        boolean kvPlanCoversBoth = kvPlan.feasible() && slotsFromKv >= slotDeficit;

        // 2. Try slot-only plan (Case 2) — check if released KV also covers kvDeficit
        EndpointFeasibility slotOnlyEf = new EndpointFeasibility(
                ef.ipPort(), RejectionReason.COMPUTE_SATURATED, 0, slotDeficit, 0);
        EvictionPlan slotPlan = planDecodeSlot(slotOnlyEf, de, incomingPriority);
        long kvFromSlots = slotPlan.victims().stream().mapToLong(PlanVictim::kvTokens).sum();
        boolean slotPlanCoversBoth = slotPlan.feasible() && kvFromSlots >= kvDeficit;

        // 3. If both cover both deficits, pick the cheaper one
        if (kvPlanCoversBoth && slotPlanCoversBoth) {
            EvictionPlan winner = kvPlan.cost() <= slotPlan.cost() ? kvPlan : slotPlan;
            Logger.info("EvictionPlanner Case 4 (combined): ep={}, both single-dim plans cover, "
                            + "winner={}, cost={}",
                    ef.ipPort(), winner == kvPlan ? "KV-only" : "slot-only", winner.cost());
            return new EvictionPlan(ef.ipPort(), ef.reason(), winner.victims(), winner.cost(), true);
        }

        // 4. If KV-only plan covers both
        if (kvPlanCoversBoth) {
            Logger.info("EvictionPlanner Case 4 (combined): ep={}, KV-only plan covers both, "
                            + "victims={}, cost={}",
                    ef.ipPort(), kvPlan.victims().size(), kvPlan.cost());
            return new EvictionPlan(ef.ipPort(), ef.reason(), kvPlan.victims(), kvPlan.cost(), true);
        }

        // 5. If slot-only plan covers both
        if (slotPlanCoversBoth) {
            Logger.info("EvictionPlanner Case 4 (combined): ep={}, slot-only plan covers both, "
                            + "victims={}, cost={}",
                    ef.ipPort(), slotPlan.victims().size(), slotPlan.cost());
            return new EvictionPlan(ef.ipPort(), ef.reason(), slotPlan.victims(), slotPlan.cost(), true);
        }

        // 6. Neither single-dimension plan covers both — combined approach
        //    (KV eviction first, then slot eviction for remaining slots)
        double totalCost = kvPlan.cost();
        List<PlanVictim> allVictims = new ArrayList<>(kvPlan.victims());

        if (slotsFromKv < slotDeficit) {
            int remainingSlots = slotDeficit - slotsFromKv;
            List<EvictionVictim> slotVictims = new ArrayList<>();

            EvictionVictim v1 = de.findUnconfirmedEvictableConcurrencyVictim(incomingPriority);
            if (v1 != null) {
                slotVictims.add(v1);
            }
            if (slotVictims.size() < remainingSlots) {
                EvictionVictim v2 = de.findRunningEvictableConcurrencyVictim(incomingPriority);
                if (v2 != null) {
                    slotVictims.add(v2);
                }
            }

            double slotCost = 0;
            for (EvictionVictim v : slotVictims) {
                int ordinal = taskPhaseToGOrdinal(v.taskPhase());
                slotCost += costFunction.f(v.priority()) * costFunction.g(ordinal);
                allVictims.add(toPlanVictim(v));
            }
            slotCost *= costFunction.h(RejectionReason.COMPUTE_SATURATED);
            totalCost += slotCost;
        }

        long totalKvFreed = allVictims.stream().mapToLong(PlanVictim::kvTokens).sum();
        boolean feasible = allVictims.size() >= slotDeficit && totalKvFreed >= kvDeficit;

        Logger.info("EvictionPlanner Case 4 (combined): ep={}, victims={}, slotDeficit={}, "
                        + "kvDeficit={}, cost={}, feasible={}",
                ef.ipPort(), allVictims.size(), slotDeficit, kvDeficit, totalCost, feasible);
        return new EvictionPlan(ef.ipPort(), ef.reason(), allVictims, totalCost, feasible);
    }

    // ==================== selectBest ====================

    /**
     * Select the minimum-cost feasible plan from the list.
     *
     * <p>Filters for plans where {@link EvictionPlan#feasible()} is true, then
     * selects the best plan using a multi-level comparator:
     * <ol>
     *   <li>Lowest cost — cheapest eviction wins.</li>
     *   <li>Fewer victims — less disruptive (tiebreaker).</li>
     *   <li>Earliest max run stage — the plan whose worst victim has the
     *       earliest run stage is less disruptive (tiebreaker).</li>
     *   <li>Endpoint name — deterministic final tiebreaker.</li>
     * </ol>
     *
     * @param plans list of plans (may include infeasible ones)
     * @return the best feasible plan, or {@code null} if no feasible plan exists
     */
    public EvictionPlan selectBest(List<EvictionPlan> plans) {
        if (plans == null || plans.isEmpty()) {
            return null;
        }
        Comparator<EvictionPlan> comparator = Comparator
                .comparingDouble(EvictionPlan::cost)
                .thenComparingInt(p -> p.victims().size())
                .thenComparingInt(this::maxVictimRunStageOrdinal)
                .thenComparing(EvictionPlan::endpointIpPort);
        return plans.stream()
                .filter(EvictionPlan::feasible)
                .min(comparator)
                .orElse(null);
    }

    /**
     * Compute the maximum run-stage ordinal among a plan's victims.
     * Used as a tiebreaker in {@link #selectBest}: a plan whose worst victim
     * has an earlier run stage (lower ordinal) is less disruptive.
     */
    private int maxVictimRunStageOrdinal(EvictionPlan plan) {
        return plan.victims().stream()
                .mapToInt(v -> taskPhaseToGOrdinal(v.taskPhase()))
                .max()
                .orElse(0);
    }

    // ==================== commit ====================

    /**
     * Execute the eviction plan: evict victims from the endpoint.
     *
     * <p>For unconfirmed decode victims: call {@link DecodeEndpoint#removeUnconfirmedVictim}.
     * For running decode victims: call {@link DecodeEndpoint#removeRunningVictim} + cancel RPC.
     * For prefill queue victims: re-find BatchItems by requestId and call
     * {@link PrefillEndpoint#evictQueuedItems}.
     *
     * @param plan     the plan to execute (must be feasible)
     * @param ctx      the incoming request's balance context
     * @param roleType the role being routed (PREFILL / DECODE)
     * @return true if eviction succeeded
     */
    public boolean commit(EvictionPlan plan, BalanceContext ctx, RoleType roleType) {
        if (plan == null || !plan.feasible() || plan.victims().isEmpty()) {
            return false;
        }

        Map<String, WorkerEndpoint> endpoints = engineWorkerStatus.selectModelWorkerStatus(roleType, null);
        WorkerEndpoint ep = endpoints.get(plan.endpointIpPort());
        if (ep == null || !ep.getStatus().isAlive()) {
            Logger.warn("EvictionPlanner.commit: endpoint not found or not alive, ipPort={}",
                    plan.endpointIpPort());
            return false;
        }

        int incomingPriority = ctx.getPriority();
        RejectionReason caseType = plan.caseType();

        if (caseType == RejectionReason.RESOURCE_UNAVAILABLE) {
            if (!(ep instanceof PrefillEndpoint pe)) {
                return false;
            }
            return commitPrefillQueue(plan, pe, incomingPriority);
        } else if (caseType == RejectionReason.COMPUTE_SATURATED) {
            if (!(ep instanceof DecodeEndpoint de)) {
                return false;
            }
            return commitDecodeVictims(plan, de, "COMPUTE_SATURATED");
        } else if (caseType == RejectionReason.KV_CAPACITY
                || caseType == RejectionReason.KV_UNAVAILABLE) {
            if (!(ep instanceof DecodeEndpoint de)) {
                return false;
            }
            return commitDecodeVictims(plan, de, "KV_FULL");
        } else {
            Logger.warn("EvictionPlanner.commit: unsupported caseType={}, ipPort={}",
                    caseType, plan.endpointIpPort());
            return false;
        }
    }

    /**
     * Commit prefill queue eviction: re-find BatchItems by requestId and evict.
     */
    private boolean commitPrefillQueue(EvictionPlan plan, PrefillEndpoint pe, int incomingPriority) {
        List<Long> victimIds = plan.victims().stream()
                .map(PlanVictim::requestId)
                .toList();

        // Re-find BatchItems (victim-finders are deterministic)
        List<BatchItem> candidates = pe.findEvictableQueuedRequests(incomingPriority, victimIds.size());
        List<BatchItem> toEvict = candidates.stream()
                .filter(item -> victimIds.contains(item.requestId()))
                .toList();

        if (toEvict.isEmpty()) {
            Logger.warn("EvictionPlanner.commit: no matching prefill victims found, ipPort={}",
                    plan.endpointIpPort());
            return false;
        }

        pe.evictQueuedItems(toEvict);
        reportEviction("PREFILL_PENDING_FULL", toEvict.size());
        Logger.info("EvictionPlanner.commit: evicted {} prefill victims from ep={}",
                toEvict.size(), plan.endpointIpPort());
        return true;
    }

    /**
     * Commit decode victim eviction (both slot and KV cases share the same removal logic).
     * Determines unconfirmed vs running based on the victim's TaskPhase.
     */
    private boolean commitDecodeVictims(EvictionPlan plan, DecodeEndpoint de, String stage) {
        int count = 0;
        for (PlanVictim v : plan.victims()) {
            if (isRunningPhase(v.taskPhase())) {
                de.removeRunningVictim(v.requestId());
                callCancel(v.requestId());
            } else {
                de.removeUnconfirmedVictim(v.requestId());
            }
            count++;
        }
        reportEviction(stage, count);
        Logger.info("EvictionPlanner.commit: evicted {} {} victims from ep={}",
                count, stage, plan.endpointIpPort());
        return count > 0;
    }

    // ==================== Helpers ====================

    /**
     * Map a {@link TaskPhase} to the run-status ordinal used by
     * {@link EvictionCostFunction#g(int)}.
     *
     * <ul>
     *   <li>PENDING / null → 0 (NOT_ACCEPTED)</li>
     *   <li>RECEIVED → 1 (ACCEPTED_NOT_RUNNING)</li>
     *   <li>KV_ALLOCATED → 1 (ACCEPTED_NOT_RUNNING)</li>
     *   <li>RUNNING → 2 (RUNNING)</li>
     * </ul>
     */
    private int taskPhaseToGOrdinal(TaskPhase phase) {
        if (phase == null || phase == TaskPhase.PENDING) {
            return 0;
        }
        if (phase == TaskPhase.RECEIVED || phase == TaskPhase.KV_ALLOCATED) {
            return 1;
        }
        return 2; // RUNNING or unknown default to most expensive
    }

    /**
     * Returns true if the phase indicates a confirmed running victim
     * (KV_ALLOCATED or RUNNING), which requires engine cancel.
     */
    private boolean isRunningPhase(TaskPhase phase) {
        return phase == TaskPhase.KV_ALLOCATED || phase == TaskPhase.RUNNING;
    }

    /**
     * Convert {@link EvictionVictim} (flexlb-sync) to {@link PlanVictim} (flexlb-common).
     */
    private PlanVictim toPlanVictim(EvictionVictim v) {
        return new PlanVictim(v.requestId(), v.kvTokens(), v.priority(), v.taskPhase());
    }

    /**
     * Call the cancel handler for a running victim (engine cancel).
     * Dispatches cancel RPC via CancelHandler with PRIORITY_PREEMPTED reason,
     * matching the pattern in {@link org.flexlb.balance.scheduler.DefaultRouter}.
     */
    private void callCancel(long requestId) {
        CancelHandler handler = cancelHandlerProvider.getIfAvailable();
        if (handler != null) {
            handler.cancel(requestId, CancelReasonPB.CANCEL_REASON_PRIORITY_PREEMPTED);
        } else {
            Logger.warn("EvictionPlanner: CancelHandler not available, skipping cancel for requestId={}",
                    requestId);
        }
        BatchSchedulerReporter reporter = reporterProvider.getIfAvailable();
        if (reporter != null) {
            reporter.reportPriorityCancel("planner");
        }
    }

    /**
     * Report a priority eviction event to the metrics reporter.
     */
    private void reportEviction(String stage, int victimCount) {
        BatchSchedulerReporter reporter = reporterProvider.getIfAvailable();
        if (reporter != null) {
            reporter.reportPriorityEvict(stage, victimCount);
        }
    }
}
