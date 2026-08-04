package org.flexlb.balance.cost;

import org.flexlb.dao.loadbalance.RejectionReason;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("DefaultEvictionCostFunction Tests")
class DefaultEvictionCostFunctionTest {

    private DefaultEvictionCostFunction fn;

    @BeforeEach
    void setUp() {
        fn = new DefaultEvictionCostFunction();
    }

    @Nested
    @DisplayName("f(priority) — exponential priority weight (B^rank)")
    class FPriorityTests {

        @Test
        @DisplayName("f(30) = 9^0 = 1.0")
        void f_returnsExponential_30() {
            assertEquals(1.0, fn.f(30));
        }

        @Test
        @DisplayName("f(40) = 9^1 = 9.0")
        void f_returnsExponential_40() {
            assertEquals(9.0, fn.f(40));
        }

        @Test
        @DisplayName("f(50) = 9^2 = 81.0")
        void f_returnsExponential_50() {
            assertEquals(81.0, fn.f(50));
        }

        @Test
        @DisplayName("f(60) = 9^3 = 729.0")
        void f_returnsExponential_60() {
            assertEquals(729.0, fn.f(60));
        }

        @Test
        @DisplayName("f(70) = 9^4 = 6561.0")
        void f_returnsExponential_70() {
            assertEquals(6561.0, fn.f(70));
        }

        @Test
        @DisplayName("f(0) = 1.0 (unknown priority defaults to rank 0)")
        void f_returnsOneForZeroPriority() {
            assertEquals(1.0, fn.f(0));
        }

        @Test
        @DisplayName("f(-1) = 1.0 (negative priority defaults to rank 0)")
        void f_returnsOneForNegativePriority() {
            assertEquals(1.0, fn.f(-1));
        }

        @Test
        @DisplayName("f(35) = 1.0 (nearest lower rank is 30 → rank 0)")
        void f_nearestLower_35() {
            assertEquals(1.0, fn.f(35));
        }

        @Test
        @DisplayName("f(45) = 9.0 (nearest lower rank is 40 → rank 1)")
        void f_nearestLower_45() {
            assertEquals(9.0, fn.f(45));
        }

        @Test
        @DisplayName("f(80) = 6561.0 (above all levels → highest rank 4)")
        void f_aboveAll_80() {
            assertEquals(6561.0, fn.f(80));
        }

        @Test
        @DisplayName("Exponential property: 8 * f(30) = 8 < f(40) = 9")
        void exponentialProperty_8VictimsRank0_lessThan_1VictimRank1() {
            double eightVictimsCost = 8 * fn.f(30);
            double oneVictimCost = fn.f(40);
            assertTrue(eightVictimsCost < oneVictimCost,
                    "8 * f(30)=" + eightVictimsCost + " should be < f(40)=" + oneVictimCost);
        }
    }

    @Nested
    @DisplayName("g(runStatusOrdinal) — run status weight (powers of 4)")
    class GRunStatusTests {

        @Test
        @DisplayName("g(0) = 1.0 (NOT_ACCEPTED)")
        void g_notAccepted() {
            assertEquals(1.0, fn.g(0));
        }

        @Test
        @DisplayName("g(1) = 4.0 (ACCEPTED_NOT_RUNNING)")
        void g_acceptedNotRunning() {
            assertEquals(4.0, fn.g(1));
        }

        @Test
        @DisplayName("g(2) = 16.0 (RUNNING)")
        void g_running() {
            assertEquals(16.0, fn.g(2));
        }

        @Test
        @DisplayName("g(3) defaults to gRunning (most expensive) for unknown ordinal")
        void g_unknownOrdinal_defaultsToRunning() {
            assertEquals(16.0, fn.g(3));
        }

        @Test
        @DisplayName("g(-1) defaults to gRunning (most expensive) for negative ordinal")
        void g_negativeOrdinal_defaultsToRunning() {
            assertEquals(16.0, fn.g(-1));
        }
    }

    @Nested
    @DisplayName("j(kvTokens) — KV tokens released advantage (ceiling bucket)")
    class JKvTokensTests {

        @Test
        @DisplayName("j(1024) = 1 (ceil(1024/1024) = 1)")
        void j_1024() {
            assertEquals(1.0, fn.j(1024));
        }

        @Test
        @DisplayName("j(2048) = 2 (ceil(2048/1024) = 2)")
        void j_2048() {
            assertEquals(2.0, fn.j(2048));
        }

        @Test
        @DisplayName("j(0) = 1 (max(1, ceil(0/1024)) = max(1, 0) = 1)")
        void j_zero() {
            assertEquals(1.0, fn.j(0));
        }

        @Test
        @DisplayName("j(512) = 1 (max(1, ceil(512/1024)) = max(1, 1) = 1)")
        void j_halfOfM() {
            assertEquals(1.0, fn.j(512));
        }

        @Test
        @DisplayName("j(1025) = 2 (max(1, ceil(1025/1024)) = max(1, 2) = 2)")
        void j_justAboveM() {
            assertEquals(2.0, fn.j(1025));
        }

        @Test
        @DisplayName("j(1) = 1 (max(1, ceil(1/1024)) = max(1, 1) = 1)")
        void j_one() {
            assertEquals(1.0, fn.j(1));
        }
    }

    @Nested
    @DisplayName("k(kvTokens) — KV token length cost (inverse of j)")
    class KKvTokensTests {

        @Test
        @DisplayName("k(1024) = 1.0 (1/j(1024) = 1/1)")
        void k_1024() {
            assertEquals(1.0, fn.k(1024));
        }

        @Test
        @DisplayName("k(512) = 1.0 (1/j(512) = 1/1)")
        void k_512() {
            assertEquals(1.0, fn.k(512));
        }

        @Test
        @DisplayName("k(2048) = 0.5 (1/j(2048) = 1/2)")
        void k_2048() {
            assertEquals(0.5, fn.k(2048));
        }

        @Test
        @DisplayName("k(0) does not throw (uses max(1,...) in j to avoid div-by-zero)")
        void k_zero_doesNotThrow() {
            assertDoesNotThrow(() -> fn.k(0));
        }

        @Test
        @DisplayName("k(0) = 1.0 (1/j(0) = 1/max(1,0) = 1/1)")
        void k_zero_returnsOne() {
            assertEquals(1.0, fn.k(0));
        }

        @Test
        @DisplayName("k(1) = 1.0 (1/j(1) = 1/max(1,1) = 1/1)")
        void k_one_returnsOne() {
            assertEquals(1.0, fn.k(1));
        }

        @Test
        @DisplayName("k(1025) = 0.5 (1/j(1025) = 1/2)")
        void k_1025() {
            assertEquals(0.5, fn.k(1025));
        }
    }

    @Nested
    @DisplayName("h(caseType) — scenario weight")
    class HCaseTypeTests {

        @Test
        @DisplayName("h(KV_CAPACITY) = 1.0 (hKv)")
        void h_kvCapacity() {
            assertEquals(1.0, fn.h(RejectionReason.KV_CAPACITY));
        }

        @Test
        @DisplayName("h(KV_UNAVAILABLE) = 1.0 (hKv)")
        void h_kvUnavailable() {
            assertEquals(1.0, fn.h(RejectionReason.KV_UNAVAILABLE));
        }

        @Test
        @DisplayName("h(COMPUTE_SATURATED) = 1.0 (hSlot)")
        void h_computeSaturated() {
            assertEquals(1.0, fn.h(RejectionReason.COMPUTE_SATURATED));
        }

        @Test
        @DisplayName("h(RESOURCE_UNAVAILABLE) = 1.0 (hSlot)")
        void h_resourceUnavailable() {
            assertEquals(1.0, fn.h(RejectionReason.RESOURCE_UNAVAILABLE));
        }

        @Test
        @DisplayName("h(NO_REGISTERED) = 0.0 (non-eviction reason)")
        void h_noRegistered() {
            assertEquals(0.0, fn.h(RejectionReason.NO_REGISTERED));
        }

        @Test
        @DisplayName("h(NOT_ALIVE) = 0.0 (non-eviction reason)")
        void h_notAlive() {
            assertEquals(0.0, fn.h(RejectionReason.NOT_ALIVE));
        }

        @Test
        @DisplayName("h(HOTSPOT_FILTERED) = 0.0 (non-eviction reason)")
        void h_hotspotFiltered() {
            assertEquals(0.0, fn.h(RejectionReason.HOTSPOT_FILTERED));
        }

        @Test
        @DisplayName("h(IMBALANCE_FILTERED) = 0.0 (non-eviction reason)")
        void h_imbalanceFiltered() {
            assertEquals(0.0, fn.h(RejectionReason.IMBALANCE_FILTERED));
        }

        @Test
        @DisplayName("h(PREDICTOR_MISSING) = 0.0 (non-eviction reason)")
        void h_predictorMissing() {
            assertEquals(0.0, fn.h(RejectionReason.PREDICTOR_MISSING));
        }

        @Test
        @DisplayName("h(SLO_VIOLATION) = 0.0 (non-eviction reason)")
        void h_sloViolation() {
            assertEquals(0.0, fn.h(RejectionReason.SLO_VIOLATION));
        }

        @Test
        @DisplayName("h(null) = 0.0")
        void h_null() {
            assertEquals(0.0, fn.h(null));
        }
    }

    @Nested
    @DisplayName("Custom configuration")
    class CustomConfigTests {

        private static final int[] DEFAULT_LEVELS = {30, 40, 50, 60, 70};

        @Test
        @DisplayName("Custom M=2048: j(1024) = 1 (max(1, ceil(1024/2048)) = 1)")
        void customM_changesJ() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    2048L, 1.0, 4.0, 16.0, 1.0, 1.0, 8, DEFAULT_LEVELS);
            assertEquals(1.0, custom.j(1024));
        }

        @Test
        @DisplayName("Custom M=2048: j(2049) = 2 (max(1, ceil(2049/2048)) = 2)")
        void customM_j_justAbove() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    2048L, 1.0, 4.0, 16.0, 1.0, 1.0, 8, DEFAULT_LEVELS);
            assertEquals(2.0, custom.j(2049));
        }

        @Test
        @DisplayName("Custom M=2048: k(1024) = 1.0 (1/j(1024) = 1/1)")
        void customM_changesK() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    2048L, 1.0, 4.0, 16.0, 1.0, 1.0, 8, DEFAULT_LEVELS);
            assertEquals(1.0, custom.k(1024));
        }

        @Test
        @DisplayName("Custom M=2048: k(0) = 1.0 (1/max(1,0) = 1/1)")
        void customM_k_zero() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    2048L, 1.0, 4.0, 16.0, 1.0, 1.0, 8, DEFAULT_LEVELS);
            assertEquals(1.0, custom.k(0));
        }

        @Test
        @DisplayName("Custom g values change g() output")
        void customG_values() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    1024L, 2.0, 8.0, 20.0, 1.0, 1.0, 8, DEFAULT_LEVELS);
            assertEquals(2.0, custom.g(0));
            assertEquals(8.0, custom.g(1));
            assertEquals(20.0, custom.g(2));
        }

        @Test
        @DisplayName("Custom hSlot and hKv change h() output")
        void customH_values() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    1024L, 1.0, 4.0, 16.0, 3.0, 7.0, 8, DEFAULT_LEVELS);
            assertEquals(7.0, custom.h(RejectionReason.KV_CAPACITY));
            assertEquals(7.0, custom.h(RejectionReason.KV_UNAVAILABLE));
            assertEquals(3.0, custom.h(RejectionReason.COMPUTE_SATURATED));
            assertEquals(3.0, custom.h(RejectionReason.RESOURCE_UNAVAILABLE));
        }

        @Test
        @DisplayName("Custom maxVictimsPerDecision=2 (B=3): f(30)=1, f(40)=3, f(50)=9")
        void customMaxVictims_changesExponentialBase() {
            DefaultEvictionCostFunction custom = new DefaultEvictionCostFunction(
                    1024L, 1.0, 4.0, 16.0, 1.0, 1.0, 2, DEFAULT_LEVELS);
            assertEquals(1.0, custom.f(30));   // 3^0
            assertEquals(3.0, custom.f(40));   // 3^1
            assertEquals(9.0, custom.f(50));   // 3^2
            assertEquals(27.0, custom.f(60));  // 3^3
            assertEquals(81.0, custom.f(70));  // 3^4
        }
    }
}
