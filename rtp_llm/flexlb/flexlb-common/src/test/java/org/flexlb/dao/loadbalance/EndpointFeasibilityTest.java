package org.flexlb.dao.loadbalance;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@DisplayName("EndpointFeasibility Record Tests")
class EndpointFeasibilityTest {

    @Nested
    @DisplayName("Record construction and field access")
    class ConstructionTests {

        @Test
        @DisplayName("All fields are accessible via accessor methods")
        void allFieldsAccessible() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 4, 2);

            assertEquals("10.0.0.1:8080", ef.ipPort());
            assertEquals(RejectionReason.KV_CAPACITY, ef.reason());
            assertEquals(512L, ef.kvDeficit());
            assertEquals(4L, ef.slotDeficit());
            assertEquals(2, ef.queueDeficit());
        }

        @Test
        @DisplayName("KV-related deficit fields default to 0 when not applicable")
        void kvDeficit_defaultsToZero() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.COMPUTE_SATURATED, 0, 8, 0);

            assertEquals(0L, ef.kvDeficit());
            assertEquals(8L, ef.slotDeficit());
            assertEquals(0, ef.queueDeficit());
        }

        @Test
        @DisplayName("Slot deficit defaults to 0 when not applicable (KV case)")
        void slotDeficit_defaultsToZero() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.KV_CAPACITY, 1024, 0, 0);

            assertEquals(1024L, ef.kvDeficit());
            assertEquals(0L, ef.slotDeficit());
            assertEquals(0, ef.queueDeficit());
        }

        @Test
        @DisplayName("Queue deficit defaults to 0 when not applicable")
        void queueDeficit_defaultsToZero() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.RESOURCE_UNAVAILABLE, 0, 0, 0);

            assertEquals(0L, ef.kvDeficit());
            assertEquals(0L, ef.slotDeficit());
            assertEquals(0, ef.queueDeficit());
        }
    }

    @Nested
    @DisplayName("Equality and hashCode")
    class EqualityTests {

        @Test
        @DisplayName("Two instances with same values are equal")
        void equalInstances() {
            EndpointFeasibility ef1 = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0);
            EndpointFeasibility ef2 = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0);

            assertEquals(ef1, ef2);
            assertEquals(ef1.hashCode(), ef2.hashCode());
        }
    }

    @Nested
    @DisplayName("Null reason handling")
    class NullReasonTests {

        @Test
        @DisplayName("Reason field can be null (no validation at construction)")
        void nullReason() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", null, 0, 0, 0);

            assertNull(ef.reason());
        }
    }

    @Nested
    @DisplayName("toString")
    class ToStringTests {

        @Test
        @DisplayName("toString contains ipPort and reason")
        void toString_containsKeyFields() {
            EndpointFeasibility ef = new EndpointFeasibility(
                    "10.0.0.1:8080", RejectionReason.KV_CAPACITY, 512, 0, 0);

            String str = ef.toString();
            assertTrue(str.contains("10.0.0.1:8080"));
            assertTrue(str.contains("KV_CAPACITY"));
        }

    }
}
