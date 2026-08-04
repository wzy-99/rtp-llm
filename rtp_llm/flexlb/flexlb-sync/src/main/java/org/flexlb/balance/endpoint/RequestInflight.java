package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.InflightEvictor;
import org.flexlb.enums.TaskPhase;

/**
 * Tracks a single inflight decode request's KV reservation.
 *
 * <p>Originally a record; converted to a class to allow mutable
 * {@code taskPhase} (calibrate write-back from {@link TaskInfo#getPhase()}).
 *
 * <ul>
 *   <li>{@code kvTokens} — hard KV demand (prompt seqLen), used for hard-capacity filtering.</li>
 *   <li>{@code expectedKvTokens} — conservative KV estimate (seqLen + maxNewTokens),
 *       used for scoring / load balancing.</li>
 *   <li>{@code createdAtMs} — epoch-millis when this entry was created.</li>
 *   <li>{@code taskPhase} — volatile, calibrate write-back of engine-side
 *       {@link TaskPhase} (PENDING/RECEIVED/KV_ALLOCATED/RUNNING).</li>
 *   <li>{@code priority} — request priority (30/40/50/60/70, default 50).
 *       Set at reserve time from {@link org.flexlb.dao.BalanceContext#getPriority()}.
 *       Used by priority-eviction victim selection (strictly less than incoming priority).</li>
 * </ul>
 */
class RequestInflight implements InflightEvictor.TtlTracked {

    private final long kvTokens;
    private final long expectedKvTokens;
    private final long createdAtMs;
    private final int priority;
    private volatile TaskPhase taskPhase;

    RequestInflight(long kvTokens, long expectedKvTokens) {
        this(kvTokens, expectedKvTokens, 50);
    }

    RequestInflight(long kvTokens, long expectedKvTokens, int priority) {
        this(kvTokens, expectedKvTokens, System.currentTimeMillis(), priority);
    }

    RequestInflight(long kvTokens, long expectedKvTokens, long createdAtMs) {
        this(kvTokens, expectedKvTokens, createdAtMs, 50);
    }

    RequestInflight(long kvTokens, long expectedKvTokens, long createdAtMs, int priority) {
        this.kvTokens = kvTokens;
        this.expectedKvTokens = expectedKvTokens;
        this.createdAtMs = createdAtMs;
        this.priority = priority;
    }

    long kvTokens() {
        return kvTokens;
    }

    long expectedKvTokens() {
        return expectedKvTokens;
    }

    int priority() {
        return priority;
    }

    @Override
    public long createdAtMs() {
        return createdAtMs;
    }

    TaskPhase getTaskPhase() {
        return taskPhase;
    }

    void setTaskPhase(TaskPhase taskPhase) {
        this.taskPhase = taskPhase;
    }

}
