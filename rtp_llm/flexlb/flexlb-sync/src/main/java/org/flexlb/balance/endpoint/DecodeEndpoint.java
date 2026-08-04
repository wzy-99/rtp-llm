package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.InflightEvictor;
import org.flexlb.dao.master.TaskInfo;
import org.flexlb.dao.master.WorkerStatus;
import org.flexlb.dao.master.WorkerStatusResponse;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.TaskPhase;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class DecodeEndpoint extends WorkerEndpoint {

    private static final org.slf4j.Logger logger = LoggerFactory.getLogger("syncLogger");

    private final ConcurrentHashMap<Long, RequestInflight> inflightRequests = new ConcurrentHashMap<>();
    /**
     * Per-request map for confirmed (KV_ALLOCATED / RUNNING) requests that have
     * not yet finished. Populated by {@link #calibrate} which moves entries here
     * from {@code inflightRequests} once the engine confirms them.
     *
     * <p>Key type is {@code Long} for consistency with {@code inflightRequests}.
     * KV reservation counters ({@code inflightKvReservedTotal} /
     * {@code inflightExpectedKvReservedTotal}) are <em>not</em> maintained for
     * entries here — they were already subtracted when the entry was moved out
     * of {@code inflightRequests} during calibrate.
     */
    private final ConcurrentHashMap<Long, RequestInflight> runningInflightRequests = new ConcurrentHashMap<>();
    private final AtomicLong inflightKvReservedTotal = new AtomicLong(0);
    private final AtomicLong inflightExpectedKvReservedTotal = new AtomicLong(0);
    private final AtomicLong reportedKvAvailable = new AtomicLong();
    private volatile int confirmedRunningCount;
    private final InflightEvictor<Long, RequestInflight> requestEvictor;

    public DecodeEndpoint(WorkerStatus status) {
        super(status);
        this.requestEvictor = new InflightEvictor<>(inflightRequests, req -> {
            inflightKvReservedTotal.addAndGet(-req.kvTokens());
            inflightExpectedKvReservedTotal.addAndGet(-req.expectedKvTokens());
        });
    }

    public void reserve(long requestId, long kvTokens, long expectedKvTokens) {
        reserve(requestId, kvTokens, expectedKvTokens, 50);
    }

    /**
     * Reserve KV tokens for a request with explicit priority.
     *
     * @param requestId       the request ID
     * @param kvTokens        hard KV demand (prompt seqLen)
     * @param expectedKvTokens conservative KV estimate (seqLen + maxNewTokens)
     * @param priority        request priority (30/40/50/60/70)
     */
    public void reserve(long requestId, long kvTokens, long expectedKvTokens, int priority) {
        RequestInflight newRi = new RequestInflight(kvTokens, expectedKvTokens, priority);
        RequestInflight prev = inflightRequests.putIfAbsent(requestId, newRi);
        if (prev != null) {
            // requestId already exists — subtract the old kvTokens before overwriting,
            // otherwise the old value is silently lost and the counter stays inflated.
            inflightKvReservedTotal.addAndGet(-prev.kvTokens());
            inflightExpectedKvReservedTotal.addAndGet(-prev.expectedKvTokens());
            inflightRequests.put(requestId, newRi);
        }
        inflightKvReservedTotal.addAndGet(kvTokens);
        inflightExpectedKvReservedTotal.addAndGet(expectedKvTokens);
    }

    /**
     * Release an inflight request by requestId.
     *
     * <p>Double-checks both maps:
     * <ol>
     *   <li>{@code runningInflightRequests} — confirmed entries whose KV reservation
     *       was already subtracted during calibrate (no counter adjustment needed).</li>
     *   <li>{@code inflightRequests} — unconfirmed entries whose KV reservation is
     *       still counted (subtract on removal).</li>
     * </ol>
     */
    public void release(long requestId) {
        RequestInflight removed = runningInflightRequests.remove(requestId);
        if (removed != null) {
            // KV reservation already subtracted in calibrate; just remove the entry.
            return;
        }
        removed = inflightRequests.remove(requestId);
        if (removed != null) {
            inflightKvReservedTotal.addAndGet(-removed.kvTokens());
            inflightExpectedKvReservedTotal.addAndGet(-removed.expectedKvTokens());
        }
    }

    @Override
    public void onWorkerStatusUpdate(WorkerStatus ws, WorkerStatusResponse resp) {
        super.onWorkerStatusUpdate(ws, resp);
        calibrate(resp.getRunningTaskInfo(), resp.getFinishedTaskInfo());
    }

    /**
     * Full calibration against worker status report.
     *
     * <p>Phase 1 (runningTaskInfo): confirmed (KV_ALLOCATED / RUNNING) requests are
     * <em>moved</em> from {@code inflightRequests} to {@code runningInflightRequests}
     * (preserving per-request tracking) and their {@code taskPhase} is written back.
     * Entries already in {@code runningInflightRequests} get their {@code taskPhase}
     * updated in-place.
     *
     * <p>Phase 2/3 (finishedTaskInfo): entries are removed from
     * {@code runningInflightRequests} first (if present), then from
     * {@code inflightRequests} as a fallback (finished before confirmation).
     */
    private void calibrate(Map<String, TaskInfo> runningTaskInfo, Map<String, TaskInfo> finishedTaskInfo) {
        this.reportedKvAvailable.set(status.getAvailableKvCacheTokens().get());

        // Phase 1: process running requests — KV_ALLOCATED or RUNNING means the engine
        // has taken ownership, so we can release our inflight reservation.
        int kvAllocatedRequests = 0;
        if (runningTaskInfo != null) {
            for (TaskInfo task : runningTaskInfo.values()) {
                TaskPhase phase = task.getPhase();
                if (phase == TaskPhase.KV_ALLOCATED || phase == TaskPhase.RUNNING) {
                    kvAllocatedRequests++;
                }
            }
        }
        this.confirmedRunningCount = kvAllocatedRequests;

        // Second pass: move confirmed tasks from inflightRequests to runningInflightRequests
        if (runningTaskInfo != null) {
            for (TaskInfo task : runningTaskInfo.values()) {
                TaskPhase phase = task.getPhase();
                if (phase == TaskPhase.KV_ALLOCATED || phase == TaskPhase.RUNNING) {
                    long reqId = task.getRequestId();
                    RequestInflight removed = inflightRequests.remove(reqId);
                    if (removed != null) {
                        removed.setTaskPhase(phase);
                        runningInflightRequests.put(reqId, removed);
                        inflightKvReservedTotal.addAndGet(-removed.kvTokens());
                        inflightExpectedKvReservedTotal.addAndGet(-removed.expectedKvTokens());
                    } else {
                        RequestInflight running = runningInflightRequests.get(reqId);
                        if (running != null) {
                            running.setTaskPhase(phase);
                        }
                    }
                }
            }
        }

        // Phase 2: process finished non-success requests
        if (finishedTaskInfo != null) {
            for (TaskInfo task : finishedTaskInfo.values()) {
                if (task.getErrorCode() != 0) {
                    long reqId = task.getRequestId();
                    RequestInflight removed = runningInflightRequests.remove(reqId);
                    if (removed == null) {
                        removed = inflightRequests.remove(reqId);
                        if (removed != null) {
                            inflightKvReservedTotal.addAndGet(-removed.kvTokens());
                            inflightExpectedKvReservedTotal.addAndGet(-removed.expectedKvTokens());
                        } else {
                            logger.debug("Decode calibrate: finished failed request reqId={} not in inflight, error={}",
                                    reqId, task.getErrorMessage());
                        }
                    }
                }
            }

            // Phase 3: process finished success requests
            for (TaskInfo task : finishedTaskInfo.values()) {
                if (task.getErrorCode() == 0) {
                    long reqId = task.getRequestId();
                    RequestInflight removed = runningInflightRequests.remove(reqId);
                    if (removed == null) {
                        removed = inflightRequests.remove(reqId);
                        if (removed != null) {
                            inflightKvReservedTotal.addAndGet(-removed.kvTokens());
                            inflightExpectedKvReservedTotal.addAndGet(-removed.expectedKvTokens());
                        }
                    }
                }
            }
        }
    }

    // ==================== KV Cache 三视图 ====================

    private long inflightKvReserved() {
        return inflightExpectedKvReservedTotal.get();
    }

    public long inflightHardKvReserved() {
        return inflightKvReservedTotal.get();
    }

    public long realKvUsed() {
        long totalCap = status.getTotalKvCacheTokens().get();
        long avail = status.getAvailableKvCacheTokens().get();
        long reportedUsed = totalCap > 0 ? Math.max(0, totalCap - avail) : 0;
        return reportedUsed + inflightKvReserved();
    }

    public long realKvAvailable() {
        return Math.max(0, reportedKvAvailable.get() - inflightHardKvReserved());
    }

    // ==================== Priority Eviction: Victim Finder ====================

    /**
     * Phase order for unconfirmed victims: PENDING (or null) = 0 (cheapest),
     * RECEIVED = 1. Lower ordinal = evict first.
     */
    private static int unconfirmedPhaseOrder(TaskPhase phase) {
        if (phase == null || phase == TaskPhase.PENDING) {
            return 0;
        }
        return 1; // RECEIVED
    }

    /**
     * Phase order for running victims: KV_ALLOCATED = 0 (cheaper),
     * RUNNING = 1 (most expensive). Lower ordinal = evict first.
     */
    private static int runningPhaseOrder(TaskPhase phase) {
        if (phase == TaskPhase.KV_ALLOCATED) {
            return 0;
        }
        return 1; // RUNNING
    }

    /**
     * Stage 3a pass1: Find evictable victims among unconfirmed inflight requests
     * (in {@code inflightRequests}, phase=PENDING/RECEIVED).
     *
     * <p>Collects entries with {@code priority < incomingPriority}, sorted by:
     * <ol>
     *   <li>Phase earliest first (PENDING > RECEIVED)</li>
     *   <li>Priority lowest first</li>
     *   <li>KV tokens largest first (greedy — fewer victims to reach needEvictKvTokens)</li>
     * </ol>
     * Greedily accumulates KV tokens until {@code >= needEvictKvTokens}.
     *
     * @param incomingPriority  the incoming request's priority (victims must be strictly lower)
     * @param needEvictKvTokens the KV tokens that must be freed
     * @return list of victims (may be empty if none found or insufficient)
     */
    public List<EvictionVictim> findUnconfirmedEvictableVictims(int incomingPriority, long needEvictKvTokens) {
        List<Map.Entry<Long, RequestInflight>> candidates = new ArrayList<>();
        for (Map.Entry<Long, RequestInflight> entry : inflightRequests.entrySet()) {
            RequestInflight ri = entry.getValue();
            if (ri.priority() < incomingPriority) {
                candidates.add(entry);
            }
        }
        if (candidates.isEmpty()) {
            return List.of();
        }
        candidates.sort(Comparator
                .comparingInt((Map.Entry<Long, RequestInflight> e) -> unconfirmedPhaseOrder(e.getValue().getTaskPhase()))
                .thenComparingInt(e -> e.getValue().priority())
                .thenComparingLong(e -> -e.getValue().kvTokens())); // desc = largest first

        List<EvictionVictim> victims = new ArrayList<>();
        long accumulated = 0;
        for (Map.Entry<Long, RequestInflight> entry : candidates) {
            RequestInflight ri = entry.getValue();
            victims.add(new EvictionVictim(entry.getKey(), ri.kvTokens(), ri.priority(), ri.getTaskPhase()));
            accumulated += ri.kvTokens();
            if (accumulated >= needEvictKvTokens) {
                break;
            }
        }
        Logger.debug("Decode findUnconfirmedEvictableVictims: ep={}, candidates={}, victims={}, accumulated={}, need={}",
                ipPort(), candidates.size(), victims.size(), accumulated, needEvictKvTokens);
        return victims;
    }

    /**
     * Stage 3a pass2: Find evictable victims among confirmed running requests
     * (in {@code runningInflightRequests}, phase=KV_ALLOCATED/RUNNING).
     *
     * <p>Same sorting and greedy logic as {@link #findUnconfirmedEvictableVictims},
     * but with running phase order (KV_ALLOCATED > RUNNING).
     *
     * @param incomingPriority  the incoming request's priority
     * @param needEvictKvTokens the KV tokens that must be freed
     * @return list of victims (may be empty)
     */
    public List<EvictionVictim> findRunningEvictableVictims(int incomingPriority, long needEvictKvTokens) {
        List<Map.Entry<Long, RequestInflight>> candidates = new ArrayList<>();
        for (Map.Entry<Long, RequestInflight> entry : runningInflightRequests.entrySet()) {
            RequestInflight ri = entry.getValue();
            if (ri.priority() < incomingPriority) {
                candidates.add(entry);
            }
        }
        if (candidates.isEmpty()) {
            return List.of();
        }
        candidates.sort(Comparator
                .comparingInt((Map.Entry<Long, RequestInflight> e) -> runningPhaseOrder(e.getValue().getTaskPhase()))
                .thenComparingInt(e -> e.getValue().priority())
                .thenComparingLong(e -> -e.getValue().kvTokens()));

        List<EvictionVictim> victims = new ArrayList<>();
        long accumulated = 0;
        for (Map.Entry<Long, RequestInflight> entry : candidates) {
            RequestInflight ri = entry.getValue();
            victims.add(new EvictionVictim(entry.getKey(), ri.kvTokens(), ri.priority(), ri.getTaskPhase()));
            accumulated += ri.kvTokens();
            if (accumulated >= needEvictKvTokens) {
                break;
            }
        }
        Logger.debug("Decode findRunningEvictableVictims: ep={}, candidates={}, victims={}, accumulated={}, need={}",
                ipPort(), candidates.size(), victims.size(), accumulated, needEvictKvTokens);
        return victims;
    }

    /**
     * Stage 3b pass1: Find a single evictable concurrency victim among unconfirmed
     * inflight requests.
     *
     * <p>Selects the victim with lowest priority and earliest phase (PENDING > RECEIVED).
     * Only one victim is needed to free one concurrency slot.
     *
     * @param incomingPriority the incoming request's priority
     * @return a single victim, or {@code null} if none found
     */
    public EvictionVictim findUnconfirmedEvictableConcurrencyVictim(int incomingPriority) {
        Map.Entry<Long, RequestInflight> best = null;
        for (Map.Entry<Long, RequestInflight> entry : inflightRequests.entrySet()) {
            RequestInflight ri = entry.getValue();
            if (ri.priority() >= incomingPriority) {
                continue;
            }
            if (best == null) {
                best = entry;
                continue;
            }
            RequestInflight bestRi = best.getValue();
            int cmp = Integer.compare(unconfirmedPhaseOrder(ri.getTaskPhase()),
                    unconfirmedPhaseOrder(bestRi.getTaskPhase()));
            if (cmp == 0) {
                cmp = Integer.compare(ri.priority(), bestRi.priority());
            }
            if (cmp < 0) {
                best = entry;
            }
        }
        if (best == null) {
            return null;
        }
        RequestInflight ri = best.getValue();
        Logger.debug("Decode findUnconfirmedEvictableConcurrencyVictim: ep={}, victim={}, priority={}, phase={}",
                ipPort(), best.getKey(), ri.priority(), ri.getTaskPhase());
        return new EvictionVictim(best.getKey(), ri.kvTokens(), ri.priority(), ri.getTaskPhase());
    }

    /**
     * Stage 3b pass2: Find a single evictable concurrency victim among confirmed
     * running requests.
     *
     * <p>Selects the victim with lowest priority and earliest phase (KV_ALLOCATED > RUNNING).
     *
     * @param incomingPriority the incoming request's priority
     * @return a single victim, or {@code null} if none found
     */
    public EvictionVictim findRunningEvictableConcurrencyVictim(int incomingPriority) {
        Map.Entry<Long, RequestInflight> best = null;
        for (Map.Entry<Long, RequestInflight> entry : runningInflightRequests.entrySet()) {
            RequestInflight ri = entry.getValue();
            if (ri.priority() >= incomingPriority) {
                continue;
            }
            if (best == null) {
                best = entry;
                continue;
            }
            RequestInflight bestRi = best.getValue();
            int cmp = Integer.compare(runningPhaseOrder(ri.getTaskPhase()),
                    runningPhaseOrder(bestRi.getTaskPhase()));
            if (cmp == 0) {
                cmp = Integer.compare(ri.priority(), bestRi.priority());
            }
            if (cmp < 0) {
                best = entry;
            }
        }
        if (best == null) {
            return null;
        }
        RequestInflight ri = best.getValue();
        Logger.debug("Decode findRunningEvictableConcurrencyVictim: ep={}, victim={}, priority={}, phase={}",
                ipPort(), best.getKey(), ri.priority(), ri.getTaskPhase());
        return new EvictionVictim(best.getKey(), ri.kvTokens(), ri.priority(), ri.getTaskPhase());
    }

    // ==================== Priority Eviction: Victim Removal ====================

    /**
     * Remove an unconfirmed victim from {@code inflightRequests} and release its
     * KV reservation. No engine cancel for PENDING phase (engine hasn't received it).
     *
     * @param requestId the victim's request ID
     * @return true if the victim was found and removed
     */
    public boolean removeUnconfirmedVictim(long requestId) {
        RequestInflight removed = inflightRequests.remove(requestId);
        if (removed != null) {
            inflightKvReservedTotal.addAndGet(-removed.kvTokens());
            inflightExpectedKvReservedTotal.addAndGet(-removed.expectedKvTokens());
            Logger.info("Decode evict unconfirmed victim: ep={}, requestId={}, kvTokens={}, priority={}, phase={}",
                    ipPort(), requestId, removed.kvTokens(), removed.priority(), removed.getTaskPhase());
            return true;
        }
        return false;
    }

    /**
     * Remove a confirmed running victim from {@code runningInflightRequests}.
     * KV reservation was already subtracted during calibrate (no counter adjustment).
     * Decrements {@code confirmedRunningCount} to reflect the freed concurrency slot.
     *
     * @param requestId the victim's request ID
     * @return true if the victim was found and removed
     */
    public boolean removeRunningVictim(long requestId) {
        RequestInflight removed = runningInflightRequests.remove(requestId);
        if (removed != null) {
            // Decrement confirmedRunningCount to free the concurrency slot.
            // calibrate will correct this on the next sync cycle.
            confirmedRunningCount = Math.max(0, confirmedRunningCount - 1);
            Logger.info("Decode evict running victim: ep={}, requestId={}, kvTokens={}, priority={}, phase={}",
                    ipPort(), requestId, removed.kvTokens(), removed.priority(), removed.getTaskPhase());
            return true;
        }
        return false;
    }

    // ==================== Metrics ====================

    public void reportBatchMetrics(BatchSchedulerReporter reporter) {
        reporter.reportInflightRequestCount(RoleType.DECODE.name(), getIp(), getInflightCount());
        reporter.reportDecodeTotalLoad(getIp(), getTotalLoad());
        reporter.reportDecodeInflightKvReserved(getIp(), inflightKvReserved());
    }

    public long realKvTotal() {
        return status.getTotalKvCacheTokens().get();
    }

    public int getInflightCount() {
        return inflightRequests.size();
    }

    public int evictExpiredRequests(long ttlMs) {
        return requestEvictor.evictExpired(ttlMs);
    }

    public int getTotalLoad() {
        return confirmedRunningCount + inflightRequests.size();
    }

    @Override
    public long getLoadMetric() {
        return getTotalLoad();
    }

}
