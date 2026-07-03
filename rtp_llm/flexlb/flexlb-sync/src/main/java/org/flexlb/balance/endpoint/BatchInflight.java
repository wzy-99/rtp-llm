package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.BatchItem;
import org.flexlb.balance.scheduler.InflightEvictor;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * DP-aware inflight batch tracker for PrefillEndpoint.
 *
 * <p>Stores requests grouped by DP rank: {@code Map<Integer, List<BatchItem>>}.
 * Single-DP scenario has one entry (rank 0). Multi-DP scenario has N entries.
 */
public final class BatchInflight implements InflightEvictor.TtlTracked {

    private final long batchId;
    private final long predictTimeMs;
    private final Map<Integer, List<BatchItem>> requestsByDpRank;
    private final long createdAtMs;
    private final AtomicLong progressBaseMs;
    private volatile boolean running;
    private volatile long lastSeenMs;

    public BatchInflight(long batchId, long predictTimeMs, Map<Integer, List<BatchItem>> requestsByDpRank) {
        this(batchId, predictTimeMs, requestsByDpRank, System.currentTimeMillis());
    }

    private BatchInflight(long batchId, long predictTimeMs, Map<Integer, List<BatchItem>> requestsByDpRank, long nowMs) {
        this(batchId, predictTimeMs, requestsByDpRank, nowMs, nowMs, false, nowMs);
    }

    private BatchInflight(long batchId,
                          long predictTimeMs,
                          Map<Integer, List<BatchItem>> requestsByDpRank,
                          long createdAtMs,
                          long progressBaseMs,
                          boolean running,
                          long lastSeenMs) {
        this.batchId = batchId;
        this.predictTimeMs = predictTimeMs;
        this.requestsByDpRank = Collections.unmodifiableMap(new LinkedHashMap<>(requestsByDpRank));
        this.createdAtMs = createdAtMs;
        this.progressBaseMs = new AtomicLong(progressBaseMs);
        this.running = running;
        this.lastSeenMs = lastSeenMs;
    }

    public long batchId() {
        return batchId;
    }

    public long predictTimeMs() {
        return predictTimeMs;
    }

    /** DP-aware request map: dpRank → requests for that DP slot. */
    public Map<Integer, List<BatchItem>> requestsByDpRank() {
        return requestsByDpRank;
    }

    /** Total request count across all DP slots. */
    public int requestCount() {
        return requestsByDpRank.values().stream().mapToInt(List::size).sum();
    }

    /** Flat view of all requests across all DP slots (for traversal). */
    public List<BatchItem> allRequests() {
        List<BatchItem> all = new ArrayList<>();
        for (List<BatchItem> slot : requestsByDpRank.values()) {
            all.addAll(slot);
        }
        return all;
    }

    @Override
    public long createdAtMs() {
        return createdAtMs;
    }

    public long progressBaseMs() {
        return progressBaseMs.get();
    }

    public boolean running() {
        return running;
    }

    public long lastSeenMs() {
        return lastSeenMs;
    }

    public void markQueued(long statusMs) {
        if (!running) {
            progressBaseMs.updateAndGet(base -> Math.max(base, statusMs));
        }
        lastSeenMs = Math.max(lastSeenMs, statusMs);
    }

    public void markRunning(long statusMs) {
        if (!running) {
            progressBaseMs.updateAndGet(base -> Math.max(base, statusMs));
            running = true;
        }
        lastSeenMs = Math.max(lastSeenMs, statusMs);
    }

    /**
     * Repack: remove specified request IDs and return a new BatchInflight with survivors.
     *
     * @param newPredictTimeMs new prediction for the surviving batch
     * @param newRequestsByDpRank filtered DP slots (survivors only)
     * @return new BatchInflight, or null if all requests removed
     */
    public BatchInflight repack(long newPredictTimeMs, Map<Integer, List<BatchItem>> newRequestsByDpRank) {
        if (newRequestsByDpRank.isEmpty()) {
            return null;
        }
        return new BatchInflight(batchId, newPredictTimeMs, newRequestsByDpRank,
                createdAtMs, progressBaseMs(), running, lastSeenMs);
    }
}
