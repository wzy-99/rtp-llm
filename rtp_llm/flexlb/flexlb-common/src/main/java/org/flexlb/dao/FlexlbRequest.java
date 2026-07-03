package org.flexlb.dao;

import lombok.Getter;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.enums.ScheduleModeEnum;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Three-path common request object — replaces the former BalanceContext God Object.
 *
 * <p>Shared by Direct, Queue, and Batch routing paths. Path-specific tracking
 * (retry count, enqueue/dequeue time, sequence id) is handled internally by
 * each path's own data structure (e.g. {@code QueueManager.QueueSlot}).
 *
 * <p>Config is retained as a transient field set by {@code RouteService} before
 * routing — routing strategies read it via {@link #getConfig()}.
 */
@Getter
public class FlexlbRequest {

    // ==================== Basic ==================== //

    private final Request request;

    @lombok.ToString.Exclude
    private byte[] generateInputPbBytes;

    private ScheduleModeEnum scheduleMode = ScheduleModeEnum.AUTO;

    /** Transient config — set by RouteService before routing, not persisted. */
    private FlexlbConfig config;

    // ==================== Async state ==================== //

    private final CompletableFuture<Response> future = new CompletableFuture<>();

    private final AtomicBoolean cancelled = new AtomicBoolean(false);

    // ==================== Metrics ==================== //

    private final long startTime = System.currentTimeMillis();

    /** Set after routing completes. */
    private volatile Response response;

    public FlexlbRequest(Request request) {
        this.request = request;
    }

    public void setGenerateInputPbBytes(byte[] bytes) {
        this.generateInputPbBytes = bytes;
    }

    public void setScheduleMode(ScheduleModeEnum scheduleMode) {
        this.scheduleMode = scheduleMode;
    }

    public void setConfig(FlexlbConfig config) {
        this.config = config;
    }

    public void setResponse(Response response) {
        this.response = response;
    }

    public long getRequestId() {
        return request.getRequestId();
    }

    /**
     * Mark request as cancelled.
     */
    public void cancel() {
        cancelled.compareAndSet(false, true);
    }

    /**
     * Check if request has been cancelled.
     */
    public boolean isCancelled() {
        return cancelled.get();
    }
}
