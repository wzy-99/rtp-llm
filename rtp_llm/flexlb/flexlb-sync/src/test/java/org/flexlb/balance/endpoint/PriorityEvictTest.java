package org.flexlb.balance.endpoint;

import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.enums.TaskPhase;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Priority eviction unit tests at the DecodeEndpoint level.
 *
 * <p>Tests victim selection logic for:
 * <ul>
 *   <li>KV_FULL eviction (unconfirmed + running)</li>
 *   <li>COMPUTE_SATURATED eviction (unconfirmed + running)</li>
 *   <li>TaskPhase sorting (PENDING preferred over RECEIVED; KV_ALLOCATED preferred over RUNNING)</li>
 *   <li>RUNNING phase not preferred (deprioritized in running victim selection)</li>
 * </ul>
 */
class PriorityEvictTest {

    private DecodeEndpoint createDecodeEndpoint() {
        WorkerStatus ws = new WorkerStatus();
        ws.setIp("10.0.0.1");
        ws.setPort(8080);
        ws.setGrpcPort(8081);
        return new DecodeEndpoint(ws);
    }

    /**
     * Use reflection to add a running inflight request directly into
     * {@code runningInflightRequests} (normally populated by calibrate).
     */
    @SuppressWarnings("unchecked")
    private void addRunningRequest(DecodeEndpoint de, long requestId, long kvTokens,
                                   int priority, TaskPhase phase) throws Exception {
        Field field = DecodeEndpoint.class.getDeclaredField("runningInflightRequests");
        field.setAccessible(true);
        Map<Long, RequestInflight> runningMap = (Map<Long, RequestInflight>) field.get(de);
        RequestInflight ri = new RequestInflight(kvTokens, kvTokens, System.currentTimeMillis(), priority);
        ri.setTaskPhase(phase);
        runningMap.put(requestId, ri);
    }

    /**
     * Use reflection to set the TaskPhase on an unconfirmed inflight entry.
     */
    @SuppressWarnings("unchecked")
    private void setUnconfirmedPhase(DecodeEndpoint de, long requestId, TaskPhase phase) throws Exception {
        Field field = DecodeEndpoint.class.getDeclaredField("inflightRequests");
        field.setAccessible(true);
        Map<Long, RequestInflight> map = (Map<Long, RequestInflight>) field.get(de);
        RequestInflight ri = map.get(requestId);
        assertNotNull(ri, "Request " + requestId + " not found in inflightRequests");
        ri.setTaskPhase(phase);
    }

    // ==================== KV_FULL eviction ====================

    @Test
    void kvFull_evicts_low_priority_unconfirmed_victim() {
        DecodeEndpoint de = createDecodeEndpoint();
        // Two unconfirmed victims with different priorities
        de.reserve(1001L, 500, 500, 30);  // low priority, 500 KV tokens
        de.reserve(1002L, 300, 300, 60);  // medium priority, 300 KV tokens

        // Need 400 KV tokens, incoming priority 70
        List<EvictionVictim> victims = de.findUnconfirmedEvictableVictims(70, 400);

        assertFalse(victims.isEmpty(), "Should find at least one victim");
        // Lower priority (30) should be evicted first
        assertEquals(1001L, victims.get(0).requestId(), "Lowest priority victim should be first");
        assertEquals(30, victims.get(0).priority());
        // Freed KV tokens should cover the need
        long freed = victims.stream().mapToLong(EvictionVictim::kvTokens).sum();
        assertTrue(freed >= 400, "Freed KV tokens (" + freed + ") should cover need (400)");
    }

    @Test
    void kvFull_evicts_low_priority_running_victim() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        addRunningRequest(de, 2001L, 500, 30, TaskPhase.RUNNING);
        addRunningRequest(de, 2002L, 300, 60, TaskPhase.RUNNING);

        // Need 400 KV tokens, incoming priority 70
        List<EvictionVictim> victims = de.findRunningEvictableVictims(70, 400);

        assertFalse(victims.isEmpty(), "Should find at least one running victim");
        assertEquals(2001L, victims.get(0).requestId(), "Lowest priority victim should be first");
        assertEquals(30, victims.get(0).priority());
    }

    @Test
    void kvFull_returns_empty_when_no_lower_priority_victims() {
        DecodeEndpoint de = createDecodeEndpoint();
        // All victims have priority >= incoming (70)
        de.reserve(1001L, 500, 500, 70);
        de.reserve(1002L, 300, 300, 80);

        List<EvictionVictim> victims = de.findUnconfirmedEvictableVictims(70, 400);
        assertTrue(victims.isEmpty(), "Should not evict same-or-higher priority requests");
    }

    // ==================== COMPUTE_SATURATED eviction ====================

    @Test
    void computeSaturated_evicts_lowest_priority_unconfirmed() {
        DecodeEndpoint de = createDecodeEndpoint();
        de.reserve(1001L, 500, 500, 30);
        de.reserve(1002L, 300, 300, 60);

        // Incoming priority 70 — only one victim needed to free a concurrency slot
        EvictionVictim victim = de.findUnconfirmedEvictableConcurrencyVictim(70);

        assertNotNull(victim, "Should find a concurrency victim");
        assertEquals(1001L, victim.requestId(), "Lowest priority victim should be selected");
        assertEquals(30, victim.priority());
    }

    @Test
    void computeSaturated_evicts_lowest_priority_running() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        addRunningRequest(de, 2001L, 500, 30, TaskPhase.RUNNING);
        addRunningRequest(de, 2002L, 300, 60, TaskPhase.RUNNING);

        EvictionVictim victim = de.findRunningEvictableConcurrencyVictim(70);

        assertNotNull(victim, "Should find a running concurrency victim");
        assertEquals(2001L, victim.requestId(), "Lowest priority victim should be selected");
        assertEquals(30, victim.priority());
    }

    @Test
    void computeSaturated_returns_null_when_no_lower_priority_victims() {
        DecodeEndpoint de = createDecodeEndpoint();
        de.reserve(1001L, 500, 500, 70);

        EvictionVictim victim = de.findUnconfirmedEvictableConcurrencyVictim(70);
        assertNull(victim, "Should not evict same-or-higher priority requests");
    }

    // ==================== TaskPhase sorting ====================

    @Test
    void taskPhaseSorting_pending_preferred_over_received() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        // Both priority=40, but different phases
        de.reserve(1001L, 500, 500, 40);
        de.reserve(1002L, 500, 500, 40);
        setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);
        setUnconfirmedPhase(de, 1002L, TaskPhase.RECEIVED);

        // PENDING (cheaper to evict — engine hasn't received it yet) should be first
        List<EvictionVictim> victims = de.findUnconfirmedEvictableVictims(70, 400);

        assertFalse(victims.isEmpty());
        assertEquals(1001L, victims.get(0).requestId(),
                "PENDING phase should be preferred over RECEIVED for eviction");
        assertEquals(TaskPhase.PENDING, victims.get(0).taskPhase());
    }

    @Test
    void taskPhaseSorting_pending_preferred_for_concurrency_victim() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        de.reserve(1001L, 500, 500, 40);
        de.reserve(1002L, 500, 500, 40);
        setUnconfirmedPhase(de, 1001L, TaskPhase.PENDING);
        setUnconfirmedPhase(de, 1002L, TaskPhase.RECEIVED);

        EvictionVictim victim = de.findUnconfirmedEvictableConcurrencyVictim(70);

        assertNotNull(victim);
        assertEquals(1001L, victim.requestId(),
                "PENDING phase should be preferred over RECEIVED for concurrency eviction");
    }

    // ==================== RUNNING phase deprioritized ====================

    @Test
    void runningPhaseOrder_kv_allocated_preferred_over_running() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        // Same priority, different running phases
        addRunningRequest(de, 2001L, 500, 40, TaskPhase.KV_ALLOCATED);
        addRunningRequest(de, 2002L, 500, 40, TaskPhase.RUNNING);

        // KV_ALLOCATED (cheaper to evict — less work done) should be preferred
        List<EvictionVictim> victims = de.findRunningEvictableVictims(70, 400);

        assertFalse(victims.isEmpty());
        assertEquals(2001L, victims.get(0).requestId(),
                "KV_ALLOCATED should be preferred over RUNNING for eviction");
        assertEquals(TaskPhase.KV_ALLOCATED, victims.get(0).taskPhase());
    }

    @Test
    void runningPhaseOrder_kv_allocated_preferred_for_concurrency_victim() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        addRunningRequest(de, 2001L, 500, 40, TaskPhase.KV_ALLOCATED);
        addRunningRequest(de, 2002L, 500, 40, TaskPhase.RUNNING);

        EvictionVictim victim = de.findRunningEvictableConcurrencyVictim(70);

        assertNotNull(victim);
        assertEquals(2001L, victim.requestId(),
                "KV_ALLOCATED should be preferred over RUNNING for concurrency eviction");
    }

    // ==================== Victim removal ====================

    @Test
    void removeUnconfirmedVictim_releases_kv_reservation() {
        DecodeEndpoint de = createDecodeEndpoint();
        de.reserve(1001L, 500, 500, 30);

        assertTrue(de.removeUnconfirmedVictim(1001L));
        assertFalse(de.removeUnconfirmedVictim(1001L), "Second removal should return false");
    }

    @Test
    void removeRunningVictim_frees_concurrency_slot() throws Exception {
        DecodeEndpoint de = createDecodeEndpoint();
        addRunningRequest(de, 2001L, 500, 30, TaskPhase.RUNNING);

        assertTrue(de.removeRunningVictim(2001L));
        assertFalse(de.removeRunningVictim(2001L), "Second removal should return false");
    }

    // ==================== Priority boundary ====================

    @Test
    void eviction_respects_strict_less_than_boundary() {
        DecodeEndpoint de = createDecodeEndpoint();
        // Victim with priority exactly equal to incoming (70) — should NOT be evicted
        de.reserve(1001L, 500, 500, 70);

        List<EvictionVictim> victims = de.findUnconfirmedEvictableVictims(70, 400);
        assertTrue(victims.isEmpty(), "Should not evict victim with priority equal to incoming");

        EvictionVictim concurrencyVictim = de.findUnconfirmedEvictableConcurrencyVictim(70);
        assertNull(concurrencyVictim, "Should not evict victim with priority equal to incoming");
    }
}
