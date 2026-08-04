package org.flexlb.balance.scheduler;

import java.util.List;

/**
 * Callback interface for {@link WorkerBatcher} to notify the scheduler of batching decisions.
 * <p>
 * Each method corresponds to a decision the batcher makes during its run loop:
 * <ul>
 *   <li>{@link #onExpired} — head item's deadline has passed, must be dropped</li>
 *   <li>{@link #onBatchReady} — a batch has been assembled and is ready for gRPC dispatch</li>
 *   <li>{@link #onOfferFailure} — a new item could not be enqueued (batcher stopped or queue full)</li>
 * </ul>
 */
public interface BatchDecisionHandler {

    /**
     * Called when the head item's SLO deadline has expired.
     * The scheduler removes it from inflight, rolls back the route, and fails the future.
     */
    void onExpired(BatchItem head);

    /**
     * Called when the batcher has assembled a batch ready for dispatch.
     */
    void onBatchReady(List<BatchItem> items, DispatchMeta meta);

    /**
     * Called when {@link WorkerBatcher#offer} fails — batcher is stopped or queue is full.
     *
     * @param item  the item that could not be enqueued
     * @param error non-null if the batcher is stopped; null if the queue is full
     */
    void onOfferFailure(BatchItem item, Throwable error);

    /**
     * Called when a queued request is in the SLO danger zone and meets
     * transfer eligibility criteria. The item has already been removed
     * from the batcher queue and its transfer count incremented. The
     * handler should roll back the current endpoint reservation and
     * re-route the request through the scheduling flow.
     *
     * @param item   the item to be transferred
     * @param reason human-readable reason (e.g. {@code "danger_zone"})
     */
    void onTransferNeeded(BatchItem item, String reason);
}
