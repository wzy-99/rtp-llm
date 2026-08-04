package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.BatchItem;
import org.flexlb.balance.scheduler.InflightEvictor;
import org.flexlb.enums.TaskPhase;

import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

final class BatchInflight implements InflightEvictor.TtlTracked {

    private final long predictTimeMs;
    private final List<BatchItem> requests;
    private final long createdAtMs;
    private final AtomicLong progressBaseMs;
    private volatile boolean running;
    private volatile TaskPhase taskPhase;

    BatchInflight(long predictTimeMs, List<BatchItem> requests) {
        this(predictTimeMs, requests, System.currentTimeMillis());
    }

    private BatchInflight(long predictTimeMs,
                          List<BatchItem> requests, long nowMs) {
        this(predictTimeMs, requests, nowMs, nowMs, false, null);
    }

    private BatchInflight(long predictTimeMs,
                          List<BatchItem> requests,
                          long createdAtMs,
                          long progressBaseMs,
                          boolean running,
                          TaskPhase taskPhase) {
        this.predictTimeMs = predictTimeMs;
        this.requests = requests;
        this.createdAtMs = createdAtMs;
        this.progressBaseMs = new AtomicLong(progressBaseMs);
        this.running = running;
        this.taskPhase = taskPhase;
    }

    long predictTimeMs() {
        return predictTimeMs;
    }

    List<BatchItem> requests() {
        return requests;
    }

    @Override
    public long createdAtMs() {
        return createdAtMs;
    }

    long progressBaseMs() {
        return progressBaseMs.get();
    }

    void markQueued(long statusMs) {
        if (!running) {
            progressBaseMs.updateAndGet(base -> Math.max(base, statusMs));
        }
    }

    void markRunning(long statusMs) {
        if (!running) {
            progressBaseMs.updateAndGet(base -> Math.max(base, statusMs));
            running = true;
        }
    }

    TaskPhase getTaskPhase() {
        return taskPhase;
    }

    void setTaskPhase(TaskPhase taskPhase) {
        this.taskPhase = taskPhase;
    }

    BatchInflight repack(long newPredictTimeMs, List<BatchItem> newRequests) {
        return new BatchInflight(newPredictTimeMs, newRequests,
                createdAtMs, progressBaseMs(), running, taskPhase);
    }
}
