package org.flexlb.balance.scheduler;

import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.enums.ScheduleModeEnum;
import reactor.core.publisher.Mono;

/**
 * Abstract base class for all routing schedulers.
 *
 * <p>Each subclass represents a distinct request dispatch path:
 * <ul>
 *   <li>{@link BatchScheduler} — batch assembly + gRPC dispatch (priority 30)</li>
 *   <li>{@link QueueScheduler} — queue + retry with worker pool (priority 20)</li>
 *   <li>{@link DirectScheduler} — synchronous worker selection (priority 10)</li>
 * </ul>
 *
 * <p>{@code RouteService} iterates schedulers in descending {@link #getOrder()}
 * and delegates to the first whose {@link #shouldHandle} returns {@code true}.
 * {@code CancelRouter} iterates all schedulers calling {@link #cancel} until
 * one returns {@code true}.
 *
 * <p>{@link #shouldHandle} is a <b>template method</b> that dispatches to
 * mode-specific hooks ({@link #shouldHandleBatch}, {@link #shouldHandleDirect},
 * {@link #shouldHandleAuto}). Subclasses override only the hooks for the modes
 * they handle; all hooks default to {@code false}.
 *
 * @author flexlb
 * @since 2026/1/15
 */
public abstract class AbstractScheduler {

    /**
     * Dispatch the request through this scheduler.
     *
     * @param request the FlexLB request to dispatch
     * @return asynchronous routing result
     */
    public abstract Mono<Response> dispatch(FlexlbRequest request);

    /**
     * Cancel a request currently in-flight through this scheduler.
     *
     * <p>Default implementation returns {@code false} (no inflight state).
     * Subclasses with inflight tracking override this method.
     *
     * @param requestId the request ID to cancel
     * @return {@code true} if the request was found and cancelled
     */
    public boolean cancel(long requestId) {
        return false;
    }

    // ==================== shouldHandle template method ====================

    /**
     * Determine whether this scheduler should handle the given request.
     *
     * <p>This is a template method that dispatches to mode-specific hooks:
     * <ul>
     *   <li>{@link ScheduleModeEnum#BATCH} → {@link #shouldHandleBatch}</li>
     *   <li>{@link ScheduleModeEnum#DIRECT} → {@link #shouldHandleDirect}</li>
     *   <li>{@link ScheduleModeEnum#AUTO} (or null) → {@link #shouldHandleAuto}</li>
     * </ul>
     *
     * <p>All hooks default to {@code false}. Subclasses override only the hooks
     * for modes they handle.
     *
     * @param request the FlexLB request
     * @param config  the current load balance config
     * @return {@code true} if this scheduler should handle the request
     */
    public boolean shouldHandle(FlexlbRequest request, FlexlbConfig config) {
        if (config == null) {
            return false;
        }
        ScheduleModeEnum mode = request.getScheduleMode();
        if (mode == ScheduleModeEnum.BATCH) {
            return shouldHandleBatch(request, config);
        }
        if (mode == ScheduleModeEnum.DIRECT) {
            return shouldHandleDirect(request, config);
        }
        return shouldHandleAuto(request, config);
    }

    /**
     * Hook: should this scheduler handle a request with explicit BATCH mode?
     *
     * <p>Default: {@code false}. Override to handle BATCH-mode requests.
     */
    protected boolean shouldHandleBatch(FlexlbRequest request, FlexlbConfig config) {
        return false;
    }

    /**
     * Hook: should this scheduler handle a request with explicit DIRECT mode?
     *
     * <p>Default: {@code false}. Override to handle DIRECT-mode requests.
     */
    protected boolean shouldHandleDirect(FlexlbRequest request, FlexlbConfig config) {
        return false;
    }

    /**
     * Hook: should this scheduler handle a request with AUTO mode?
     *
     * <p>Default: {@code false}. Override to handle AUTO-mode requests.
     * This is the primary hook for most schedulers, as AUTO is the default mode.
     */
    protected boolean shouldHandleAuto(FlexlbRequest request, FlexlbConfig config) {
        return false;
    }

    // ==================== priority ====================

    /**
     * Scheduler priority for selection ordering.
     *
     * <p>Higher values are evaluated first.
     *
     * @return priority value
     */
    public abstract int getOrder();
}
