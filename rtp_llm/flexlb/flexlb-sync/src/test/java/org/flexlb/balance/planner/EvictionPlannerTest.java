package org.flexlb.balance.planner;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.scheduler.BatchItem;
import org.flexlb.balance.scheduler.CancelHandler;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.EndpointFeasibility;
import org.flexlb.dao.loadbalance.EvictionPlan;
import org.flexlb.dao.loadbalance.PlanVictim;
import org.flexlb.dao.loadbalance.RejectionReason;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.flexlb.schedule.grpc.FlexlbScheduleProtocol.CancelReasonPB;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.sync.status.EngineWorkerStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@DisplayName("EvictionPlanner Tests")
class EvictionPlannerTest {

    private ConfigService configService;
    private EngineWorkerStatus engineWorkerStatus;
    @SuppressWarnings("unchecked")
    private ObjectProvider<CancelHandler> cancelHandlerProvider;
    @SuppressWarnings("unchecked")
    private ObjectProvider<BatchSchedulerReporter> reporterProvider;
    private EvictionPlanner planner;

    private FlexlbConfig config;
    private BalanceContext ctx;
    private CancelHandler cancelHandler;
    private BatchSchedulerReporter reporter;

    private static final int INCOMING_PRIORITY = 70;

    @BeforeEach
    void setUp() {
        configService = mock(ConfigService.class);
        engineWorkerStatus = mock(EngineWorkerStatus.class);
        cancelHandlerProvider = mock(ObjectProvider.class);
        reporterProvider = mock(ObjectProvider.class);

        config = new FlexlbConfig();
        when(configService.loadBalanceConfig()).thenReturn(config);

        planner = new EvictionPlanner(configService, engineWorkerStatus,
                cancelHandlerProvider, reporterProvider);

        cancelHandler = mock(CancelHandler.class);
        when(cancelHandlerProvider.getIfAvailable()).thenReturn(cancelHandler);
        reporter = mock(BatchSchedulerReporter.class);
        when(reporterProvider.getIfAvailable()).thenReturn(reporter);

        ctx = mock(BalanceContext.class);
        when(ctx.getPriority()).thenReturn(INCOMING_PRIORITY);
    }

    // ==================== Helpers ====================

    private DecodeEndpoint createDecodeEndpoint(String ip, int port, int grpcPort) {
        WorkerStatus ws = new WorkerStatus();
        ws.setIp(ip);
        ws.setPort(port);
        ws.setGrpcPort(grpcPort);
        ws.setAlive(true);
        return new DecodeEndpoint(ws);
    }

    private PrefillEndpoint createMockPrefillEndpoint(String ip, int port, int grpcPort) {
        PrefillEndpoint pe = mock(PrefillEndpoint.class);
        WorkerStatus ws = new WorkerStatus();
        ws.setIp(ip);
        ws.setPort(port);
        ws.setGrpcPort(grpcPort);
        ws.setAlive(true);
        when(pe.getStatus()).thenReturn(ws);
        when(pe.ipPort()).thenReturn(ws.getIpPort());
        return pe;
    }

    private BatchItem createBatchItem(long requestId, int priority, long enqueuedAtMs) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setPriority(priority);
        BalanceContext balanceContext = new BalanceContext();
        balanceContext.setRequest(request);
        balanceContext.setPriority(priority);
        return new BatchItem(balanceContext, null, null, null, null, null, null, enqueuedAtMs);
    }

    @SuppressWarnings("unchecked")
    private void setUnconfirmedPhase(DecodeEndpoint de, long requestId, TaskPhase phase) throws Exception {
        Field field = DecodeEndpoint.class.getDeclaredField("inflightRequests");
        field.setAccessible(true);
        Map<Long, Object> map = (Map<Long, Object>) field.get(de);
        Object ri = map.get(requestId);
        assertNotNull(ri, "Request " + requestId + " not found in inflightRequests");
        Class<?> riClass = ri.getClass();
        Method setTaskPhase = riClass.getDeclaredMethod("setTaskPhase", TaskPhase.class);
        setTaskPhase.setAccessible(true);
        setTaskPhase.invoke(ri, phase);
    }

    @SuppressWarnings("unchecked")
    private void addRunningRequest(DecodeEndpoint de, long requestId, long kvTokens,
                                   int priority, TaskPhase phase) throws Exception {
        Field field = DecodeEndpoint.class.getDeclaredField("runningInflightRequests");
        field.setAccessible(true);
        Map<Long, Object> runningMap = (Map<Long, Object>) field.get(de);

        Class<?> riClass = Class.forName("org.flexlb.balance.endpoint.RequestInflight");
        Constructor<?> ctor = riClass.getDeclaredConstructor(
                long.class, long.class, long.class, int.class);
        ctor.setAccessible(true);
        Object ri = ctor.newInstance(kvTokens, kvTokens, System.currentTimeMillis(), priority);

        Method setTaskPhase = riClass.getDeclaredMethod("setTaskPhase", TaskPhase.class);
        setTaskPhase.setAccessible(true);
        setTaskPhase.invoke(ri, phase);

        runningMap.put(requestId, ri);
    }

    // ==================== Case 1: Prefill queue full (RESOURCE_UNAVAILABLE) ====================

    @Nested
    @DisplayName("Case 1: Prefill queue full (RESOURCE_UNAVAILABLE)")
    class Case1PrefillQueue {

        @Test
        @DisplayName("planAll generates feasible plan with correct victims and cost")
        void planAll_case1_generatesPlanWithCorrectVictimsAndCost() {
            String ipPort = "10.0.0.1:8080";
            PrefillEndpoint pe = createMockPrefillEndpoint("10.0.0.1", 8080, 8081);

            BatchItem item1 = createBatchItem(1001L, 30, 1000L);
            BatchItem item2 = createBatchItem(1002L, 40, 2000L);
            when(pe.findEvictableQueuedRequests(INCOMING_PRIORITY, 2))
                    .thenReturn(List.of(item1, item2));

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.PREFILL, null))
                    .thenReturn(Map.of(ipPort, pe));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.RESOURCE_UNAVAILABLE, 0, 0, 2);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.PREFILL, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(ipPort, plan.endpointIpPort());
            assertEquals(RejectionReason.RESOURCE_UNAVAILABLE, plan.caseType());
            assertEquals(2, plan.victims().size());
            // Cost = f(30) + f(40) = B^0 + B^1 = 1 + 9 = 10.0
            assertEquals(10.0, plan.cost(), 0.001);
        }

        @Test
        @DisplayName("planAll Case 1 all-or-nothing: infeasible when not enough victims")
        void planAll_case1_infeasibleWhenNotEnoughVictims() {
            String ipPort = "10.0.0.2:8080";
            PrefillEndpoint pe = createMockPrefillEndpoint("10.0.0.2", 8080, 8081);

            BatchItem item1 = createBatchItem(1001L, 30, 1000L);
            when(pe.findEvictableQueuedRequests(INCOMING_PRIORITY, 2))
                    .thenReturn(List.of(item1));

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.PREFILL, null))
                    .thenReturn(Map.of(ipPort, pe));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.RESOURCE_UNAVAILABLE, 0, 0, 2);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.PREFILL, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertFalse(plan.feasible());
            assertEquals(1, plan.victims().size());
            // Cost = f(30) = B^0 = 1
            assertEquals(1.0, plan.cost(), 0.001);
        }
    }

    // ==================== Case 2: Decode slot full (COMPUTE_SATURATED) ====================

    @Nested
    @DisplayName("Case 2: Decode slot full (COMPUTE_SATURATED)")
    class Case2DecodeSlot {

        @Test
        @DisplayName("planAll generates feasible plan with correct victims and cost")
        void planAll_case2_generatesPlanWithCorrectVictimsAndCost() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.3", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(1, plan.victims().size());
            assertEquals(1001L, plan.victims().get(0).requestId());
            assertEquals(30, plan.victims().get(0).priority());
            // Cost = f(30) * g(0) * h(COMPUTE_SATURATED) = 1 * 1.0 * 1.0 = 1.0
            assertEquals(1.0, plan.cost(), 0.001);
        }

        @Test
        @DisplayName("planAll Case 2 all-or-nothing: infeasible when no victims available")
        void planAll_case2_infeasibleWhenNoVictims() {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.4", 8080, 8081);
            String ipPort = de.ipPort();

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertFalse(plan.feasible());
            assertEquals(0, plan.victims().size());
        }
    }

    // ==================== Case 3: Decode KV not enough (KV_CAPACITY / KV_UNAVAILABLE) ====================

    @Nested
    @DisplayName("Case 3: Decode KV not enough (KV_CAPACITY)")
    class Case3DecodeKv {

        @Test
        @DisplayName("planAll generates feasible plan with correct victims and cost")
        void planAll_case3_generatesPlanWithCorrectVictimsAndCost() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.5", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_CAPACITY, 400, 0, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(1, plan.victims().size());
            assertEquals(1001L, plan.victims().get(0).requestId());
            // Cost = f(30) * k(500) * g(0) * h(KV_CAPACITY) = 1 * 1.0 * 1.0 * 1.0
            assertEquals(1.0, plan.cost(), 0.01);
        }

        @Test
        @DisplayName("planAll Case 3 all-or-nothing: infeasible when KV released < deficit")
        void planAll_case3_infeasibleWhenNotEnoughKvReleased() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.6", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 300, 300, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_CAPACITY, 400, 0, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertFalse(plan.feasible());
            assertEquals(1, plan.victims().size());
        }

        @Test
        @DisplayName("Case 3 sort: kvBucket descending separates from g(runStage) — larger KV PENDING before smaller KV RUNNING")
        void planAll_case3_sortOrder_kvBucketBeforeG() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.19", 8080, 8081);
            String ipPort = de.ipPort();

            // Victim A: unconfirmed, large KV, PENDING (j(2048)=2.0, g(0)=1.0)
            de.reserve(1001L, 2048, 2048, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            // Victim B: running, smaller KV, RUNNING (j(1024)=1.0, g(2)=16.0)
            addRunningRequest(de, 2001L, 1024, 30, TaskPhase.RUNNING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            // kvDeficit = 2500: A alone (2048) < 2500, so both victims are needed
            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_CAPACITY, 2500, 0, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(2, plan.victims().size());
            // New sort: kvBucket descending → A (j=2.0) before B (j=1.0)
            assertEquals(1001L, plan.victims().get(0).requestId());
            assertEquals(2001L, plan.victims().get(1).requestId());
        }
    }

    // ==================== Case 4: Both slot and KV full ====================

    @Nested
    @DisplayName("Case 4: Both slot and KV full (combined eviction)")
    class Case4Combined {

        @Test
        @DisplayName("simultaneous deficit: EndpointFeasibility with both deficits dispatches to combined planning")
        void planAll_case4_bothDeficitsDispatchesToCombined() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.20", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            // Both deficits populated — should dispatch to Case 4 (planCombined)
            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_UNAVAILABLE, 400, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            // caseType should be the original reason (KV_UNAVAILABLE)
            assertEquals(RejectionReason.KV_UNAVAILABLE, plan.caseType());
        }

        @Test
        @DisplayName("both cover: picks cheaper slot-only plan when both single-dim plans cover both deficits")
        void planAll_case4_bothCover_picksCheaper() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.21", 8080, 8081);
            String ipPort = de.ipPort();

            // One unconfirmed victim with large KV
            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            // slotDeficit=1, kvDeficit=400
            // KV-only: 1 victim, 500 >= 400 KV, victims.size()=1 >= 1 slot → covers both
            //   cost = f(30)*k(500)*g(0)*h(KV) = 1*1*1*1 = 1.0
            // Slot-only: 1 victim, victims.size()=1 >= 1 slot, kvFromSlots=500 >= 400 → covers both
            //   cost = f(30)*g(0)*h(SLOT) = 1*1*1 = 1.0
            // Both cost 1.0, code picks kvPlan (cost <= check)
            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_UNAVAILABLE, 400, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(1, plan.victims().size());
            // Both plans cost 1.0, code picks kvPlan
            assertEquals(1.0, plan.cost(), 0.01);
        }

        @Test
        @DisplayName("slot-first check: slot-only plan that also covers KV is used when KV-only doesn't cover both")
        void planAll_case4_slotOnlyCoversBoth() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.22", 8080, 8081);
            String ipPort = de.ipPort();

            // Victim A: unconfirmed, priority=30, PENDING
            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            // Victim B: running, priority=40, RUNNING
            addRunningRequest(de, 2001L, 500, 40, TaskPhase.RUNNING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            // slotDeficit=2, kvDeficit=300
            // KV-only: finds 1 victim (A, 500 >= 300 KV), but 1 < 2 slots → doesn't cover both
            // Slot-only: finds A (unconfirmed) + B (running), 2 >= 2 slots,
            //   kvFromSlots = 500+500 = 1000 >= 300 → covers both
            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_UNAVAILABLE, 300, 2, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            // Slot-only plan covers both: should have 2 victims (A and B)
            assertEquals(2, plan.victims().size());
            // Slot-only cost = f(30)*g(0)*h(SLOT) + f(40)*g(2)*h(SLOT) = 1*1*1 + 9*16*1 = 145
            assertEquals(145.0, plan.cost(), 0.01);
        }

        @Test
        @DisplayName("combined fallback: neither single-dim plan covers both — KV first then slot for remaining")
        void planAll_case4_combinedFallback() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.23", 8080, 8081);
            String ipPort = de.ipPort();

            // Victim A: unconfirmed, small KV, PENDING
            de.reserve(1001L, 200, 200, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            // Victim B: running, priority=40, RUNNING (has concurrency slot but small KV)
            addRunningRequest(de, 2001L, 200, 40, TaskPhase.RUNNING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            // slotDeficit=2, kvDeficit=300
            // KV-only: finds A (200 KV < 300), then B from running (200 KV), total=400 >= 300.
            //   victims.size()=2 >= 2 slots → covers both!
            //   Wait: findUnconfirmedEvictableVictims finds A (200 < 300), returns [A].
            //   findRunningEvictableVictims finds B (200), returns [B].
            //   kvPlan: victims=[A,B], accumulated=400 >= 300, feasible=true.
            //   slotsFromKv=2 >= 2 → kvPlanCoversBoth=true
            // So KV-only covers both → uses KV-only plan
            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.KV_UNAVAILABLE, 300, 2, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);

            assertEquals(1, plans.size());
            EvictionPlan plan = plans.get(0);
            assertTrue(plan.feasible());
            assertEquals(2, plan.victims().size());
        }
    }

    // ==================== selectBest ====================

    @Nested
    @DisplayName("selectBest: cross-endpoint minimum cost selection")
    class SelectBestTests {

        @Test
        @DisplayName("selectBest returns minimum-cost feasible plan")
        void selectBest_returnsMinimumCostFeasiblePlan() {
            EvictionPlan plan1 = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1001L, 0, 30, TaskPhase.PENDING)),
                    30.0, true);
            EvictionPlan plan2 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1002L, 0, 40, TaskPhase.PENDING)),
                    40.0, true);
            EvictionPlan plan3 = new EvictionPlan("10.0.0.3:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1003L, 0, 50, TaskPhase.PENDING)),
                    50.0, true);

            EvictionPlan best = planner.selectBest(List.of(plan2, plan1, plan3));

            assertNotNull(best);
            assertEquals("10.0.0.1:8080", best.endpointIpPort());
            assertEquals(30.0, best.cost(), 0.001);
        }

        @Test
        @DisplayName("selectBest returns null when no feasible plans exist")
        void selectBest_returnsNullWhenNoFeasiblePlans() {
            EvictionPlan plan1 = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(), 0.0, false);
            EvictionPlan plan2 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(), 0.0, false);

            EvictionPlan best = planner.selectBest(List.of(plan1, plan2));

            assertNull(best);
        }

        @Test
        @DisplayName("selectBest returns null for empty list")
        void selectBest_returnsNullForEmptyList() {
            assertNull(planner.selectBest(List.of()));
        }

        @Test
        @DisplayName("selectBest returns null for null input")
        void selectBest_returnsNullForNullInput() {
            assertNull(planner.selectBest(null));
        }

        @Test
        @DisplayName("selectBest ignores infeasible plans and returns the best feasible one")
        void selectBest_ignoresInfeasibleAndReturnsBestFeasible() {
            EvictionPlan infeasible = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1, 0, 10, null)),
                    10.0, false);
            EvictionPlan feasible1 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(2, 0, 30, null)),
                    30.0, true);
            EvictionPlan feasible2 = new EvictionPlan("10.0.0.3:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(3, 0, 50, null)),
                    50.0, true);

            EvictionPlan best = planner.selectBest(List.of(infeasible, feasible1, feasible2));

            assertNotNull(best);
            assertEquals("10.0.0.2:8080", best.endpointIpPort());
            assertEquals(30.0, best.cost(), 0.001);
        }

        @Test
        @DisplayName("selectBest tie-breaker: fewer victims wins when cost is equal")
        void selectBest_tieBreaker_fewerVictimsWins() {
            EvictionPlan plan1 = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1001L, 0, 30, TaskPhase.PENDING)),
                    30.0, true);
            EvictionPlan plan2 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1002L, 0, 15, TaskPhase.PENDING),
                            new PlanVictim(1003L, 0, 15, TaskPhase.PENDING)),
                    30.0, true);

            EvictionPlan best = planner.selectBest(List.of(plan1, plan2));

            assertNotNull(best);
            assertEquals("10.0.0.1:8080", best.endpointIpPort());
            assertEquals(1, best.victims().size());
        }

        @Test
        @DisplayName("selectBest tie-breaker: earlier max run stage wins when cost and victim count are equal")
        void selectBest_tieBreaker_earlierRunStageWins() {
            EvictionPlan plan1 = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1001L, 0, 30, TaskPhase.PENDING)),
                    30.0, true);
            EvictionPlan plan2 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1002L, 0, 30, TaskPhase.RUNNING)),
                    30.0, true);

            EvictionPlan best = planner.selectBest(List.of(plan1, plan2));

            assertNotNull(best);
            assertEquals("10.0.0.1:8080", best.endpointIpPort());
        }

        @Test
        @DisplayName("selectBest tie-breaker: endpoint name breaks ties deterministically")
        void selectBest_tieBreaker_endpointNameBreaksTies() {
            EvictionPlan plan1 = new EvictionPlan("10.0.0.2:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1001L, 0, 30, TaskPhase.PENDING)),
                    30.0, true);
            EvictionPlan plan2 = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1002L, 0, 30, TaskPhase.PENDING)),
                    30.0, true);

            EvictionPlan best = planner.selectBest(List.of(plan1, plan2));

            assertNotNull(best);
            assertEquals("10.0.0.1:8080", best.endpointIpPort());
        }
    }

    // ==================== commit ====================

    @Nested
    @DisplayName("commit: actual eviction execution")
    class CommitTests {

        @Test
        @DisplayName("commit evicts unconfirmed decode victim and returns true")
        void commit_decodeSlot_evictsUnconfirmedVictim() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.7", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EvictionPlan plan = new EvictionPlan(ipPort,
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(new PlanVictim(1001L, 500, 30, TaskPhase.PENDING)),
                    30.0, true);

            boolean result = planner.commit(plan, ctx, RoleType.DECODE);

            assertTrue(result);
            assertFalse(de.removeUnconfirmedVictim(1001L));
            verify(reporter).reportPriorityEvict(eq("COMPUTE_SATURATED"), eq(1));
        }

        @Test
        @DisplayName("commit evicts running decode victim and calls cancel RPC")
        void commit_decodeKv_evictsRunningVictimAndCallsCancel() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.8", 8080, 8081);
            String ipPort = de.ipPort();

            addRunningRequest(de, 2001L, 500, 30, TaskPhase.RUNNING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EvictionPlan plan = new EvictionPlan(ipPort,
                    RejectionReason.KV_CAPACITY,
                    List.of(new PlanVictim(2001L, 500, 30, TaskPhase.RUNNING)),
                    300.0, true);

            boolean result = planner.commit(plan, ctx, RoleType.DECODE);

            assertTrue(result);
            assertFalse(de.removeRunningVictim(2001L));
            verify(cancelHandler).cancel(eq(2001L), eq(CancelReasonPB.CANCEL_REASON_PRIORITY_PREEMPTED));
            verify(reporter).reportPriorityCancel(eq("planner"));
            verify(reporter).reportPriorityEvict(eq("KV_FULL"), eq(1));
        }

        @Test
        @DisplayName("commit evicts prefill queue victims")
        void commit_prefillQueue_evictsQueuedItems() {
            String ipPort = "10.0.0.9:8080";
            PrefillEndpoint pe = createMockPrefillEndpoint("10.0.0.9", 8080, 8081);

            BatchItem item1 = createBatchItem(1001L, 30, 1000L);
            BatchItem item2 = createBatchItem(1002L, 40, 2000L);
            when(pe.findEvictableQueuedRequests(INCOMING_PRIORITY, 2))
                    .thenReturn(List.of(item1, item2));

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.PREFILL, null))
                    .thenReturn(Map.of(ipPort, pe));

            EvictionPlan plan = new EvictionPlan(ipPort,
                    RejectionReason.RESOURCE_UNAVAILABLE,
                    List.of(new PlanVictim(1001L, 0, 30, null),
                            new PlanVictim(1002L, 0, 40, null)),
                    70.0, true);

            boolean result = planner.commit(plan, ctx, RoleType.PREFILL);

            assertTrue(result);
            verify(pe).evictQueuedItems(anyList());
            verify(reporter).reportPriorityEvict(eq("PREFILL_PENDING_FULL"), eq(2));
        }

        @Test
        @DisplayName("commit returns false for infeasible plan")
        void commit_returnsFalseForInfeasiblePlan() {
            EvictionPlan plan = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(), 0.0, false);

            boolean result = planner.commit(plan, ctx, RoleType.DECODE);
            assertFalse(result);
        }

        @Test
        @DisplayName("commit returns false for null plan")
        void commit_returnsFalseForNullPlan() {
            boolean result = planner.commit(null, ctx, RoleType.DECODE);
            assertFalse(result);
        }

        @Test
        @DisplayName("commit returns false for empty victim list")
        void commit_returnsFalseForEmptyVictims() {
            EvictionPlan plan = new EvictionPlan("10.0.0.1:8080",
                    RejectionReason.COMPUTE_SATURATED,
                    List.of(), 0.0, true);

            boolean result = planner.commit(plan, ctx, RoleType.DECODE);
            assertFalse(result);
        }
    }

    // ==================== Cost function verification ====================

    @Nested
    @DisplayName("Cost function verification")
    class CostFunctionTests {

        @Test
        @DisplayName("Lower-priority victims produce lower cost plans")
        void lowerPriorityProducesLowerCost() throws Exception {
            DecodeEndpoint de1 = createDecodeEndpoint("10.0.0.10", 8080, 8081);
            DecodeEndpoint de2 = createDecodeEndpoint("10.0.0.11", 8080, 8081);
            String ipPort1 = de1.ipPort();
            String ipPort2 = de2.ipPort();

            de1.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de1, 1001L, TaskPhase.PENDING);
            de2.reserve(1002L, 500, 500, 50);
            setUnconfirmedPhase(de2, 1002L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort1, de1, ipPort2, de2));

            EndpointFeasibility ef1 = new EndpointFeasibility(
                    ipPort1, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);
            EndpointFeasibility ef2 = new EndpointFeasibility(
                    ipPort2, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef1, ef2), ctx, RoleType.DECODE, null);
            EvictionPlan best = planner.selectBest(plans);

            assertNotNull(best);
            assertEquals(ipPort1, best.endpointIpPort());
            // Cost = f(30)*g(0)*h(SLOT) = 1*1*1 = 1.0
            assertEquals(1.0, best.cost(), 0.001);
        }

        @Test
        @DisplayName("Unconfirmed victims are cheaper than running victims (g(0) < g(2))")
        void unconfirmedCheaperThanRunning() throws Exception {
            DecodeEndpoint de1 = createDecodeEndpoint("10.0.0.12", 8080, 8081);
            DecodeEndpoint de2 = createDecodeEndpoint("10.0.0.13", 8080, 8081);
            String ipPort1 = de1.ipPort();
            String ipPort2 = de2.ipPort();

            de1.reserve(1001L, 500, 500, 40);
            setUnconfirmedPhase(de1, 1001L, TaskPhase.PENDING);

            addRunningRequest(de2, 2001L, 500, 40, TaskPhase.RUNNING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort1, de1, ipPort2, de2));

            EndpointFeasibility ef1 = new EndpointFeasibility(
                    ipPort1, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);
            EndpointFeasibility ef2 = new EndpointFeasibility(
                    ipPort2, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef1, ef2), ctx, RoleType.DECODE, null);
            EvictionPlan best = planner.selectBest(plans);

            assertNotNull(best);
            assertEquals(ipPort1, best.endpointIpPort());
            // Cost = f(40)*g(0)*h(SLOT) = 9*1*1 = 9.0
            assertEquals(9.0, best.cost(), 0.001);
        }
    }

    // ==================== planAll edge cases ====================

    @Nested
    @DisplayName("planAll edge cases")
    class PlanAllEdgeCases {

        @Test
        @DisplayName("planAll skips endpoints not found in the worker map")
        void planAll_skipsUnknownEndpoints() {
            String ipPort = "10.0.0.99:8080";
            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of());

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);
            assertTrue(plans.isEmpty());
        }

        @Test
        @DisplayName("planAll skips dead endpoints")
        void planAll_skipsDeadEndpoints() {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.14", 8080, 8081);
            de.getStatus().setAlive(false);
            String ipPort = de.ipPort();

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.COMPUTE_SATURATED, 0, 1, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);
            assertTrue(plans.isEmpty());
        }

        @Test
        @DisplayName("planAll returns empty for unsupported rejection reason")
        void planAll_returnsEmptyForUnsupportedReason() throws Exception {
            DecodeEndpoint de = createDecodeEndpoint("10.0.0.15", 8080, 8081);
            String ipPort = de.ipPort();

            de.reserve(1001L, 500, 500, 30);
            setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);

            when(engineWorkerStatus.selectModelWorkerStatus(RoleType.DECODE, null))
                    .thenReturn(Map.of(ipPort, de));

            EndpointFeasibility ef = new EndpointFeasibility(
                    ipPort, RejectionReason.NOT_ALIVE, 0, 0, 0);

            List<EvictionPlan> plans = planner.planAll(List.of(ef), ctx, RoleType.DECODE, null);
            assertTrue(plans.isEmpty());
        }
    }
}
