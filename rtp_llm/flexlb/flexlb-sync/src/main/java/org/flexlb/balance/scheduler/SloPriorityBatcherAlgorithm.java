package org.flexlb.balance.scheduler;

import org.flexlb.balance.strategy.PrefillTimePredictor;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SLO-priority batching algorithm (v4) that combines FixedWindow's batching
 * pipeline with priority+deadline ordering.
 *
 * <h3>Design</h3>
 * <p>Inherits the 7-step dispatch pipeline from {@link FixedWindowBatcherAlgorithm}
 * (oversized rejection, queue deadline drop, engine backpressure, batch-full,
 * fixed-window timeout, predictor-based early dispatch, park). The key difference
 * is the sort key encoding:
 * <ul>
 *   <li>{@code computeSortKey} = {@code (MAX_PRIORITY - priority) * SCALE + deadline},
 *       where {@code deadline = enqueuedAtMs + sloMs - predMs}.</li>
 *   <li>Higher priority &rarr; lower sortKey &rarr; earlier in the
 *       {@link java.util.concurrent.PriorityBlockingQueue} (min-heap).</li>
 *   <li>Same priority &rarr; earlier deadline &rarr; lower sortKey &rarr;
 *       earlier in queue.</li>
 *   <li>{@code pickWithinCapacity} iterates {@link BatcherContext#sortedItems()}
 *       which respects this ordering, so batches are filled with the
 *       highest-priority, earliest-deadline items first.</li>
 * </ul>
 *
 * <h3>Key differences from {@link FixedWindowBatcherAlgorithm}</h3>
 * <ul>
 *   <li>Sort key encodes priority + SLO deadline, not FIFO arrival timestamp.</li>
 *   <li>High-priority requests jump ahead of low-priority requests in the queue.</li>
 *   <li>Within the same priority level, earlier-deadline requests are served first.</li>
 * </ul>
 *
 * <h3>Key differences from {@link SloBudgetBatcherAlgorithm}</h3>
 * <ul>
 *   <li>Uses the full FixedWindow 7-step pipeline (including queue deadline drop
 *       and oversized rejection), not the 5-step urgent-window assembly.</li>
 *   <li>Priority is encoded directly in the sort key, not via a separate
 *       urgent-window split.</li>
 *   <li>Simpler implementation &mdash; no urgent/outside split, no SLO check
 *       during batch expansion.</li>
 * </ul>
 */
public class SloPriorityBatcherAlgorithm implements BatcherAlgorithm {

    /**
     * Scale factor that separates priority bands in the sort key.
     * Must be larger than the maximum possible deadline timestamp
     * (current epoch ms is ~1.7&times;10<sup>12</sup>, so 10<sup>13</sup>
     * provides safe headroom).
     *
     * <p>Note: a three-level encoding
     * {@code (MAX_PRIORITY - priority) * LARGE + deadline * MEDIUM + enqueuedAtMs}
     * would overflow {@code long} because deadline and enqueuedAtMs are both
     * epoch-millisecond scale (~10<sup>12</sup>). Instead, enqueuedAtMs is
     * implicitly encoded through the deadline
     * ({@code deadline = enqueuedAtMs + sloMs - predMs}), and the
     * {@link WorkerBatcher} queue comparator adds
     * {@code .thenComparingLong(BatchItem::enqueuedAtMs)} as an explicit
     * third tiebreaker for the rare case of same priority + same deadline
     * but different enqueuedAtMs (due to different sloMs).
     */
    private static final long SCALE = 10_000_000_000_000L; // 10^13

    /**
     * Maximum priority value used in the sort key encoding.
     * Priority levels are 30/40/50/60/70; 100 provides headroom.
     */
    private static final int MAX_PRIORITY = 100;

    @Override
    public long computeSortKey(BatcherContext ctx, BatchItem item) {
        int priority = item.priority();
        long baseSlo = ctx.cfg().lookupLengthIntervalSlo(item.seqLen());
        double multiplier = ctx.cfg().lookupPriorityMultiplier(priority);
        long sloMs = (long) (baseSlo * multiplier);
        long predMs = estimatePredictedMs(ctx, item);
        long deadline = item.enqueuedAtMs() + sloMs - predMs;
        item.setDeadline(deadline);
        return (long) (MAX_PRIORITY - priority) * SCALE + deadline;
    }

    /**
     * Estimate how long an incoming request would wait in the priority queue
     * before being dispatched. Called by the strategy layer (via
     * {@link WorkerBatcher#estimateQueueWaitMs}) to make SLO-based eviction
     * decisions.
     *
     * <p>The estimate is based on how many queued items are ahead of (or tied
     * with) the incoming request in priority-deadline order, divided by the
     * batch size, multiplied by the average dispatch interval.
     *
     * <h3>Algorithm</h3>
     * <ol>
     *   <li>Compute the incoming sortKey using the same formula as
     *       {@link #computeSortKey}:
     *       {@code (MAX_PRIORITY - incomingPriority) * SCALE + incomingDeadlineMs}.</li>
     *   <li>Count items in the queue with {@code sortKey <= incomingSortKey}
     *       (ahead of or tied with the incoming request).</li>
     *   <li>If the queue is empty or no items are ahead &rarr; return 0
     *       (dispatched immediately).</li>
     *   <li>If adding the incoming would complete a batch
     *       ({@code size % batchSize == batchSize - 1}) &rarr; return 0
     *       (batch_full triggers immediately).</li>
     *   <li>Otherwise:
     *       {@code ceil(itemsAhead / batchSize) * averageDispatchInterval},
     *       capped at {@code flexlbBatchEnqueueDeadlineMs}.</li>
     * </ol>
     *
     * @param ctx                batcher context (provides queue access and config)
     * @param incomingPriority   priority of the incoming request
     * @param incomingDeadlineMs deadline of the incoming request
     *                           (= {@code now + sloMs - predMs})
     * @param incomingSeqLen     sequence length of the incoming request
     *                           (reserved for predictor-based refinement)
     * @param incomingCacheHit    cache hit ratio of the incoming request
     *                           (reserved for future use)
     * @return estimated wait time in milliseconds, or 0 if the request would
     *         be dispatched immediately
     */
    public long estimateQueueWaitMs(BatcherContext ctx, int incomingPriority,
                                   long incomingDeadlineMs, int incomingSeqLen,
                                   double incomingCacheHit) {
        int batchSize = Math.max(1, ctx.cfg().getFlexlbBatchSizeMax());
        long averageDispatchInterval = ctx.cfg().getFlexlbBatchFixedWaitMs();
        long capMs = ctx.cfg().getFlexlbBatchEnqueueDeadlineMs();

        // 1. Empty queue -- incoming would be dispatched immediately.
        if (ctx.isEmpty()) {
            return 0;
        }

        // 2. Compute the incoming sortKey using the same formula as
        //    computeSortKey.
        long incomingSortKey = (long) (MAX_PRIORITY - incomingPriority) * SCALE + incomingDeadlineMs;

        // 3. Count items currently in the queue with sortKey <= incoming's
        //    sortKey (ahead of or tied with the incoming request).
        int itemsAhead = 0;
        for (BatchItem item : ctx.sortedItems()) {
            if (item.sortKey() <= incomingSortKey) {
                itemsAhead++;
            }
        }

        // 4. No items ahead -- incoming is highest priority, dispatched next.
        if (itemsAhead == 0) {
            return 0;
        }

        // 5. If adding the incoming would complete a batch (batch_full),
        //    the batch is dispatched immediately on the next processQueue call.
        if (batchSize > 1 && ctx.size() % batchSize == batchSize - 1) {
            return 0;
        }

        // 6. Estimate: how many batch cycles must pass before the incoming
        //    is dispatched?
        long estimatedWait = (long) Math.ceil((double) itemsAhead / batchSize)
                * averageDispatchInterval;

        // 7. Cap at a reasonable maximum (flexlbBatchEnqueueDeadlineMs).
        if (capMs > 0) {
            estimatedWait = Math.min(estimatedWait, capMs);
        }

        return estimatedWait;
    }

    @Override
    public long queueWaitMs(BatcherContext ctx) {
        long now = ctx.now();
        long fixedWaitMs = ctx.cfg().getFlexlbBatchFixedWaitMs();
        int batchMaxCount = Math.max(1, ctx.cfg().getFlexlbBatchSizeMax());

        // Empty queue -- new request starts a new batch cycle
        if (ctx.isEmpty()) {
            if (batchMaxCount <= 1) {
                return 0;
            }
            return fixedWaitMs;
        }

        BatchItem head = ctx.peek();
        if (head == null) {
            // Race: queue was drained between isEmpty() and peek()
            return fixedWaitMs;
        }

        // batchMaxCount == 1: each request is its own batch, dispatched immediately
        if (batchMaxCount <= 1) {
            return 0;
        }

        long elapsedMs = now - head.enqueuedAtMs();
        int queueSize = ctx.size();

        // New request would fill a batch -> batch_full triggers immediately
        if (queueSize % batchMaxCount == batchMaxCount - 1) {
            return 0;
        }

        // Queue depth < batchMaxCount and window already timed out -> immediate dispatch
        if (queueSize < batchMaxCount && elapsedMs >= fixedWaitMs) {
            return 0;
        }

        // Queue depth < batchMaxCount and window not yet expired -> wait remaining window
        if (queueSize < batchMaxCount) {
            return Math.max(0, fixedWaitMs - elapsedMs);
        }

        // Queue depth >= batchMaxCount, new request doesn't fill the last batch.
        // Conservative upper-bound estimate under O(1) constraint.
        return fixedWaitMs;
    }

    @Override
    public void processQueue(BatcherContext ctx) throws InterruptedException {
        if (ctx.isEmpty()) {
            return;
        }

        // Step 0: Transfer scan — check for danger-zone requests that meet
        // transfer eligibility criteria and re-route them before normal
        // dispatch logic runs.
        FlexlbConfig config = ctx.cfg();
        if (config.isSloTransferEnabled()) {
            long now = ctx.now();
            List<BatchItem> transferCandidates = new ArrayList<>();
            for (BatchItem item : ctx.snapshot()) {
                if (item.getTransferCount() >= config.getSloTransferMaxCount()) {
                    continue;
                }
                if (item.priority() < config.getSloTransferMinPriority()) {
                    continue;
                }
                long remaining = item.deadline() - now;
                if (remaining < config.getSloDangerThresholdMs()) {
                    transferCandidates.add(item);
                }
            }
            if (!transferCandidates.isEmpty()) {
                for (BatchItem item : transferCandidates) {
                    ctx.remove(item);
                    item.incrementTransferCount();
                    ctx.handler().onTransferNeeded(item, "danger_zone");
                }
                return;
            }
        }

        BatchItem head = ctx.peek();
        if (head == null) {
            return;
        }

        long elapsedMs = ctx.now() - head.enqueuedAtMs();
        long fixedWaitMs = ctx.cfg().getFlexlbBatchFixedWaitMs();
        int batchMaxCount = Math.max(1, ctx.cfg().getFlexlbBatchSizeMax());
        long predictThresholdMs = ctx.cfg().getFlexlbBatchPredictThresholdMs();
        long batchMaxTokens = ctx.batchTokenCapacity();

        // 0. Oversized head rejection: if the head request's seqLen exceeds
        //    batchMaxCapacity, it can never be picked by any batch, so drop it
        //    immediately instead of waiting for the queue deadline.
        if (!BatchShape.empty().add(head).fitsCompute(batchMaxTokens)) {
            ctx.rejectForBatchTokenCapacity(head, batchMaxTokens);
            return;
        }

        // 1. Queue deadline: drop the head request if it has waited longer
        //    than the enqueue deadline. Runs BEFORE backpressure to ensure
        //    stale requests are cleared even under sustained backpressure.
        long queueDeadlineMs = ctx.cfg().getFlexlbBatchEnqueueDeadlineMs();
        if (queueDeadlineMs > 0 && elapsedMs > queueDeadlineMs) {
            Logger.warn("flexlb_batch_drop request_id={} reason=queue_deadline_exceeded "
                            + "elapsed_ms={} deadline_ms={} priority={}",
                    head.requestId(), elapsedMs, queueDeadlineMs, head.priority());
            ctx.dropHead(head);
            return;
        }

        // 2. Engine backpressure: park if the prefill worker already has too
        //    many batches inflight, to prevent overloading the engine.
        int maxInflightBatches = ctx.cfg().getFlexlbBatchFixedMaxInflightBatches();
        if (maxInflightBatches > 0
                && ctx.prefillEp().getInflightBatchCount() >= maxInflightBatches) {
            TimeUnit.MILLISECONDS.sleep(1);
            return;
        }

        // 3. Batch full: queue size >= batchMaxCount -> dispatch immediately.
        //    pickWithinCapacity iterates ctx.sortedItems() which respects the
        //    priority+deadline sort key.
        if (ctx.size() >= batchMaxCount) {
            List<BatchItem> picked = pickWithinCapacity(
                    ctx, batchMaxCount, batchMaxTokens, ctx.batchKvCapacity());
            if (!picked.isEmpty()) {
                dispatch(ctx, picked, "batch_full");
            }
            return;
        }

        // 4. Fixed window timeout: head has waited >= fixedWaitMs -> dispatch
        //    whatever has accumulated (up to batch size limit).
        if (elapsedMs >= fixedWaitMs) {
            List<BatchItem> picked = pickWithinCapacity(
                    ctx, batchMaxCount, batchMaxTokens, ctx.batchKvCapacity());
            if (!picked.isEmpty()) {
                dispatch(ctx, picked, "fixed_window_timeout");
            }
            return;
        }

        // 5. Predictor-based early dispatch: if the predictor estimates the
        //    accumulated batch will take at least predictThresholdMs, dispatch
        //    immediately rather than waiting for the window to expire.
        if (predictThresholdMs > 0) {
            PrefillTimePredictor predictor = ctx.prefillEp().getPredictor();
            List<BatchItem> candidates = pickWithinCapacity(
                    ctx, batchMaxCount, batchMaxTokens, ctx.batchKvCapacity());
            if (!candidates.isEmpty()
                    && predictor.predictBatchMs(candidates) >= predictThresholdMs) {
                dispatch(ctx, candidates, "predict_threshold");
                return;
            }
        }

        // 6. Park briefly and retry on the next loop iteration.
        TimeUnit.MILLISECONDS.sleep(1);
    }

    // ==================== Internal helpers ====================

    /**
     * Estimate single-request prefill execution time using the predictor
     * from the prefill endpoint.
     *
     * @param ctx  batcher context
     * @param item the request
     * @return predicted execution time in ms, or 0 if no predictor is available
     */
    private long estimatePredictedMs(BatcherContext ctx, BatchItem item) {
        PrefillTimePredictor predictor = ctx.prefillEp().getPredictor();
        if (predictor == null) {
            return 0;
        }
        return predictor.estimateMs(item.seqLen(), 0);
    }

    /**
     * Greedily pick up to {@code maxCount} items in sort-key order while keeping
     * the batch inside the Engine's compute and KV resource shape.
     *
     * <p>Copied from {@link FixedWindowBatcherAlgorithm}. Iterates
     * {@link BatcherContext#sortedItems()} which respects the priority+deadline
     * sort key, so the highest-priority, earliest-deadline items are picked
     * first. The FIFO head is never rejected on dynamic KV availability:
     * temporary KV pressure only prevents adding more members to this batch.
     */
    private static List<BatchItem> pickWithinCapacity(BatcherContext ctx,
                                                       int maxCount,
                                                       long batchMaxTokens,
                                                       long batchKvTokens) {
        List<BatchItem> picked = new ArrayList<>();
        BatchShape shape = BatchShape.empty();
        for (BatchItem item : ctx.sortedItems()) {
            if (picked.size() >= maxCount) {
                break;
            }
            BatchShape candidate = shape.add(item);
            if (!candidate.fitsCompute(batchMaxTokens)) {
                break;
            }
            if (!picked.isEmpty() && !candidate.fitsKv(batchKvTokens)) {
                break;
            }
            picked.add(item);
            shape = candidate;
        }
        return picked;
    }

    private static void dispatch(BatcherContext ctx, List<BatchItem> picked, String reason) {
        BatchItem head = picked.get(0);
        long waitMs = ctx.now() - head.enqueuedAtMs();

        ctx.reporter().reportDispatchReason(
                RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason);
        ctx.reporter().reportBatchSize(
                RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason, picked.size());

        // Compute batch-aggregated cache hit ratio
        long totalSeqLen = 0;
        long totalHitCache = 0;
        for (BatchItem item : picked) {
            totalSeqLen += item.seqLen();
            totalHitCache += item.hitCache();
        }
        ctx.reporter().reportBatchCacheHitMetrics(
                RoleType.PREFILL.name(), ctx.prefillEp().getIp(), totalHitCache, totalSeqLen);
        ctx.reporter().reportBatchTotalTokens(
                RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason, totalSeqLen);

        Logger.debug("flexlb_batch_decision reason={} picked_size={} "
                        + "wait_ms={} queue_before={} worker={} head_req_id={} head_priority={}",
                reason, picked.size(), waitMs, ctx.size(), ctx.key(),
                head.requestId(), head.priority());

        ctx.dispatch(picked,
                new DispatchMeta(reason, ctx.size() - picked.size()));
    }
}
