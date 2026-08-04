package org.flexlb.dao.loadbalance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("ServerStatus Feasibility Tests")
class ServerStatusFeasibilityTest {

    @Nested
    @DisplayName("failureWithFeasibility — rejections derivation")
    class FailureWithFeasibilityTests {

        @Test
        @DisplayName("Two feasibilities with same reason → getRejections() returns {reason: 2}")
        void derivesCorrectRejections_sameReason() {
            List<EndpointFeasibility> feasibilities = List.of(
                    new EndpointFeasibility("10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0),
                    new EndpointFeasibility("10.0.0.2:8080", RejectionReason.KV_CAPACITY, 1024, 0, 0)
            );

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_AVAILABLE_WORKER, feasibilities);

            Map<RejectionReason, Integer> rejections = status.getRejections();
            assertEquals(1, rejections.size());
            assertEquals(2, rejections.get(RejectionReason.KV_CAPACITY));
        }

        @Test
        @DisplayName("Feasibilities with different reasons → getRejections() returns correct counts")
        void derivesCorrectRejections_differentReasons() {
            List<EndpointFeasibility> feasibilities = List.of(
                    new EndpointFeasibility("10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0),
                    new EndpointFeasibility("10.0.0.2:8080", RejectionReason.COMPUTE_SATURATED, 0, 4, 0),
                    new EndpointFeasibility("10.0.0.3:8080", RejectionReason.KV_CAPACITY, 256, 0, 0),
                    new EndpointFeasibility("10.0.0.4:8080", RejectionReason.KV_UNAVAILABLE, 0, 0, 0)
            );

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_AVAILABLE_WORKER, feasibilities);

            Map<RejectionReason, Integer> rejections = status.getRejections();
            assertEquals(3, rejections.size());
            assertEquals(2, rejections.get(RejectionReason.KV_CAPACITY));
            assertEquals(1, rejections.get(RejectionReason.COMPUTE_SATURATED));
            assertEquals(1, rejections.get(RejectionReason.KV_UNAVAILABLE));
        }

        @Test
        @DisplayName("Empty feasibilities list → getRejections() returns empty map")
        void derivesCorrectRejections_emptyList() {
            List<EndpointFeasibility> feasibilities = Collections.emptyList();

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_AVAILABLE_WORKER, feasibilities);

            Map<RejectionReason, Integer> rejections = status.getRejections();
            assertTrue(rejections.isEmpty());
        }

        @Test
        @DisplayName("failureWithFeasibility sets endpointFeasibilities on the status")
        void setsFeasibilities() {
            List<EndpointFeasibility> feasibilities = List.of(
                    new EndpointFeasibility("10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0),
                    new EndpointFeasibility("10.0.0.2:8080", RejectionReason.COMPUTE_SATURATED, 0, 4, 0)
            );

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_AVAILABLE_WORKER, feasibilities);

            assertEquals(feasibilities, status.getEndpointFeasibilities());
            assertEquals(2, status.getEndpointFeasibilities().size());
        }

        @Test
        @DisplayName("failureWithFeasibility sets failure fields (success=false, code, message)")
        void setsFailureFields() {
            List<EndpointFeasibility> feasibilities = List.of(
                    new EndpointFeasibility("10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0)
            );

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_DECODE_WORKER, feasibilities);

            assertEquals(false, status.isSuccess());
            assertEquals(StrategyErrorType.NO_DECODE_WORKER.getErrorCode(), status.getCode());
            assertEquals(StrategyErrorType.NO_DECODE_WORKER.getErrorMsg(), status.getMessage());
        }
    }

    @Nested
    @DisplayName("failure() — rejections fallback to stored field")
    class FailureFallbackTests {

        @Test
        @DisplayName("When endpointFeasibilities is empty (via failure()), getRejections() returns stored map")
        void getRejections_fallsBackToStoredField() {
            Map<RejectionReason, Integer> storedRejections = Map.of(
                    RejectionReason.KV_CAPACITY, 3,
                    RejectionReason.COMPUTE_SATURATED, 1
            );

            ServerStatus status = ServerStatus.failure(
                    StrategyErrorType.NO_AVAILABLE_WORKER, storedRejections);

            Map<RejectionReason, Integer> rejections = status.getRejections();
            assertEquals(2, rejections.size());
            assertEquals(3, rejections.get(RejectionReason.KV_CAPACITY));
            assertEquals(1, rejections.get(RejectionReason.COMPUTE_SATURATED));
        }

        @Test
        @DisplayName("failure() with empty rejections map → getRejections() returns empty")
        void getRejections_emptyStoredMap() {
            ServerStatus status = ServerStatus.failure(
                    StrategyErrorType.NO_AVAILABLE_WORKER, Collections.emptyMap());

            assertTrue(status.getRejections().isEmpty());
        }

        @Test
        @DisplayName("failure() does not set endpointFeasibilities (stays empty)")
        void failure_doesNotSetFeasibilities() {
            Map<RejectionReason, Integer> storedRejections = Map.of(
                    RejectionReason.KV_CAPACITY, 1
            );

            ServerStatus status = ServerStatus.failure(
                    StrategyErrorType.NO_AVAILABLE_WORKER, storedRejections);

            assertTrue(status.getEndpointFeasibilities().isEmpty());
        }
    }

    @Nested
    @DisplayName("Priority: feasibilities take precedence over stored rejections")
    class PrecedenceTests {

        @Test
        @DisplayName("When feasibilities is non-empty, getRejections() derives from feasibilities, not stored field")
        void feasibilities_takePrecedence() {
            // Create a status with both feasibilities and rejections set
            List<EndpointFeasibility> feasibilities = List.of(
                    new EndpointFeasibility("10.0.0.1:8080", RejectionReason.COMPUTE_SATURATED, 0, 2, 0),
                    new EndpointFeasibility("10.0.0.2:8080", RejectionReason.COMPUTE_SATURATED, 0, 1, 0)
            );

            ServerStatus status = ServerStatus.failureWithFeasibility(
                    StrategyErrorType.NO_AVAILABLE_WORKER, feasibilities);

            // Manually set a different rejections map — should be ignored
            status.setRejections(Map.of(RejectionReason.KV_CAPACITY, 99));

            Map<RejectionReason, Integer> rejections = status.getRejections();
            // Should derive from feasibilities, not from the stored rejections
            assertEquals(1, rejections.size());
            assertEquals(2, rejections.get(RejectionReason.COMPUTE_SATURATED));
        }
    }
}
