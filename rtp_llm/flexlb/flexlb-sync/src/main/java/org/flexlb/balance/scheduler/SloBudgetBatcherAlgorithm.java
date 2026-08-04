package org.flexlb.balance.scheduler;

import org.flexlb.balance.strategy.PrefillTimePredictor;
import org.flexlb.dao.route.RoleType;
import org.flexlb.util.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * SLO-deadline-aware batching algorithm with priority-based urgent-window
 * dispatch, copied and adapted from {@link FixedWindowBatcherAlgorithm}.
 *
 * <h3>Design (Stage 1 — soft priority, no eviction)</h3>
 * <p>Structure (window, {@code fixed_window_timeout}, {@code processQueue}
 * framework) is copied from FixedWindow. On top of that:
 * <ul>
 *   <li>{@code computeSortKey} = {@code now + sloMs - predMs} (deadline),
 *       not FIFO. {@code sloMs} via {@code FlexlbConfig.resolveSloMs(seqLen)},
 *       {@code predMs} via {@link FormulaPredictor}.</li>
 *   <li>5-step urgent-window batch assembly driven by
 *       {@code BatchItem.priority} (see {@link #processQueue}).</li>
 *   <li><b>No dropHead / no inflight_full_guard</b> — backpressure is park-only
 *       (inherited from FixedWindow). Deadline overflow requests fall into the
 *       urgent window and are dispatched first; running-time eviction is handled
 *       by Stage 3 priority eviction (Phase 4+); endpoint TTL is the final
 *       safety net. Priority determines who is evicted, not deadline.</li>
 * </ul>
 *
 * <h3>Key differences from {@link FixedWindowBatcherAlgorithm}</h3>
 * <ul>
 *   <li>Sort key is SLO deadline, not arrival timestamp.</li>
 *   <li>Dispatch picking uses 5-step urgent-window priority algorithm.</li>
 *   <li>No queue-deadline drop — overdue requests are urgent, not dropped.</li>
 *   <li>Urgent high-priority backlog triggers fast dispatch (no
 *       {@code fixedWaitMs} wait).</li>
 * </ul>
 */
public class SloBudgetBatcherAlgorithm implements BatcherAlgorithm {

    @Override
    public long computeSortKey(BatcherContext ctx, BatchItem item) {
        long now = System.currentTimeMillis();
        long sloMs = ctx.cfg().resolveSloMs(item.seqLen());
        PrefillTimePredictor predictor = ctx.prefillEp().getPredictor();
        long predMs = predictor.estimateMs(item.seqLen(), item.hitCache());
        return now + sloMs - predMs;
    }

    @Override
    public long queueWaitMs(BatcherContext ctx) {
        // Copied from FixedWindow — window-based wait estimation using
        // enqueuedAtMs (not deadline) for window timing.
        long now = ctx.now();
        long fixedWaitMs = ctx.cfg().getFlexlbBatchFixedWaitMs();
        int batchMaxCount = Math.max(1, ctx.cfg().getFlexlbBatchSizeMax());

        if (ctx.isEmpty()) {
            if (batchMaxCount <= 1) {
                return 0;
            }
            return fixedWaitMs;
        }

        BatchItem head = ctx.peek();
        if (head == null) {
            return fixedWaitMs;
        }

        if (batchMaxCount <= 1) {
            return 0;
        }

        long elapsedMs = now - head.enqueuedAtMs();
        int queueSize = ctx.size();

        if (queueSize % batchMaxCount == batchMaxCount - 1) {
            return 0;
        }

        if (queueSize < batchMaxCount && elapsedMs >= fixedWaitMs) {
            return 0;
        }

        if (queueSize < batchMaxCount) {
            return Math.max(0, fixedWaitMs - elapsedMs);
        }

        return fixedWaitMs;
    }

    @Override
    public void processQueue(BatcherContext ctx) throws InterruptedException {
        if (ctx.isEmpty()) {
            return;
        }

        BatchItem head = ctx.peek();
        if (head == null) {
            return;
        }

        long now = ctx.now();
        long fixedWaitMs = ctx.cfg().getFlexlbBatchFixedWaitMs();
        int batchMaxCount = Math.max(1, ctx.cfg().getFlexlbBatchSizeMax());
        long predictThresholdMs = ctx.cfg().getFlexlbBatchPredictThresholdMs();
        long batchMaxTokens = ctx.batchTokenCapacity();
        long batchKvTokens = ctx.batchKvCapacity();
        long urgentRangeMs = ctx.cfg().getFlexlbSloUrgentRangeMs();
        int defaultPriority = ctx.cfg().getFlexlbPriorityDefault();

        // Oversized head rejection (same as FixedWindow — impossible to batch)
        if (!BatchShape.empty().add(head).fitsCompute(batchMaxTokens)) {
            ctx.rejectForBatchTokenCapacity(head, batchMaxTokens);
            return;
        }

        // Backpressure: park if inflight batches >= max.
        // No dropHead, no inflight_full_guard — just park (copy from FixedWindow).
        // Deadline overflow is handled by urgent-window dispatch, not by dropping.
        int maxInflightBatches = ctx.cfg().getFlexlbBatchSloMaxInflightBatches();
        if (maxInflightBatches > 0 && ctx.prefillEp().getInflightBatchCount() >= maxInflightBatches) {
            TimeUnit.MILLISECONDS.sleep(1);
            return;
        }

        // ---- Determine dispatch triggers (copied from FixedWindow + urgent fast dispatch) ----

        long elapsedMs = now - head.enqueuedAtMs();
        boolean batchFull = ctx.size() >= batchMaxCount;
        boolean windowTimeout = elapsedMs >= fixedWaitMs;

        // Get all items sorted by deadline (sortKey)
        List<BatchItem> allItems = ctx.sortedItems();

        // Step 1: Split into urgent window (deadline - now < urgentRangeMs) and outside
        long urgentThreshold = now + urgentRangeMs;
        List<BatchItem> urgent = new ArrayList<>();
        List<BatchItem> outside = new ArrayList<>();
        for (BatchItem item : allItems) {
            if (item.sortKey() < urgentThreshold) {
                urgent.add(item);
            } else {
                outside.add(item);
            }
        }

        // Check if urgent window has high-priority backlog (priority > default)
        boolean hasUrgentHighPriority = !urgent.isEmpty() && urgent.stream()
                .anyMatch(item -> item.priority() > defaultPriority);

        // Urgent fast dispatch: high-priority items in urgent window → dispatch immediately
        boolean urgentFastDispatch = hasUrgentHighPriority;

        // Check predictor-based early dispatch (same as FixedWindow, only if no main trigger)
        boolean predictTriggered = false;
        if (!batchFull && !windowTimeout && !urgentFastDispatch && predictThresholdMs > 0) {
            PrefillTimePredictor predictor = ctx.prefillEp().getPredictor();
            List<BatchItem> candidates = pickWithinCapacity(
                    allItems, batchMaxCount, batchMaxTokens, batchKvTokens);
            if (!candidates.isEmpty() && predictor.predictBatchMs(candidates) >= predictThresholdMs) {
                predictTriggered = true;
            }
        }

        // No trigger → park
        if (!batchFull && !windowTimeout && !urgentFastDispatch && !predictTriggered) {
            TimeUnit.MILLISECONDS.sleep(1);
            return;
        }

        // ---- 5-step urgent-window batch assembly ----

        // Step 2: Sort urgent by priority desc, then deadline asc
        urgent.sort(Comparator.comparingInt(BatchItem::priority).reversed()
                .thenComparingLong(BatchItem::sortKey));

        List<BatchItem> picked;
        String reason;

        if (urgent.isEmpty()) {
            // Step 3: empty urgent window → original FixedWindow deadline FIFO
            picked = pickWithinCapacity(allItems, batchMaxCount, batchMaxTokens, batchKvTokens);
            reason = predictTriggered ? "predict_threshold"
                    : (batchFull ? "batch_full" : "fixed_window_timeout");
        } else {
            // Step 2: pick urgent items first (priority desc, deadline asc)
            picked = pickWithinCapacity(urgent, batchMaxCount, batchMaxTokens, batchKvTokens);

            if (picked.size() >= batchMaxCount) {
                // Step 5: urgent items fill the batch
                reason = hasUrgentHighPriority ? "urgent_priority_full" : "batch_full";
            } else {
                // Step 4: try adding outside items if all requests can still meet SLO
                PrefillTimePredictor predictor = ctx.prefillEp().getPredictor();
                List<BatchItem> expanded = tryAddOutsideWithSloCheck(
                        picked, outside, batchMaxCount, batchMaxTokens, batchKvTokens,
                        predictor, now);

                if (expanded.size() > picked.size()) {
                    // Outside items added — batch filled with urgent + outside
                    picked = expanded;
                    reason = "urgent_with_outside_fill";
                } else {
                    // Step 5: can't add outside without breaking SLO → only urgent
                    reason = hasUrgentHighPriority ? "urgent_priority_fast_dispatch"
                            : (predictTriggered ? "predict_threshold" : "fixed_window_timeout");
                }
            }
        }

        if (!picked.isEmpty()) {
            dispatch(ctx, picked, reason);
        } else {
            TimeUnit.MILLISECONDS.sleep(1);
        }
    }

    // ==================== Internal helpers ====================

    /**
     * Greedily pick up to {@code maxCount} items from the candidate list
     * (already sorted by the caller) while keeping the batch inside the
     * Engine's compute and KV resource shape.
     *
     * <p>Copied from {@link FixedWindowBatcherAlgorithm#pickWithinCapacity}.
     * The FIFO head is never rejected on dynamic KV availability: temporary
     * KV pressure only prevents adding more members to this batch.
     */
    private static List<BatchItem> pickWithinCapacity(List<BatchItem> candidates,
                                                       int maxCount,
                                                       long batchMaxTokens,
                                                       long batchKvTokens) {
        List<BatchItem> picked = new ArrayList<>();
        BatchShape shape = BatchShape.empty();
        for (BatchItem item : candidates) {
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

    /**
     * Step 4: Try adding outside-window items to the batch while ensuring
     * all items (including urgent ones) can still meet their SLO deadlines.
     *
     * <p>Outside items are considered in deadline order only (no priority).
     * If adding an outside item would cause any request in the batch to miss
     * its SLO deadline (predicted batch time exceeds remaining slack), we
     * stop adding outside items — only urgent items are dispatched.
     *
     * @param urgentPicked  Items already picked from the urgent window
     * @param outside       Outside-window candidates (sorted by deadline)
     * @param maxCount      Maximum batch size
     * @param batchMaxTokens Compute token capacity
     * @param batchKvTokens  KV token capacity
     * @param predictor      Prefill time predictor
     * @param now            Current timestamp
     * @return Expanded list (may be same as urgentPicked if no outside items added)
     */
    private static List<BatchItem> tryAddOutsideWithSloCheck(
            List<BatchItem> urgentPicked,
            List<BatchItem> outside,
            int maxCount,
            long batchMaxTokens,
            long batchKvTokens,
            PrefillTimePredictor predictor,
            long now) {
        List<BatchItem> picked = new ArrayList<>(urgentPicked);
        BatchShape shape = BatchShape.empty();
        for (BatchItem item : urgentPicked) {
            shape = shape.add(item);
        }

        for (BatchItem item : outside) {
            if (picked.size() >= maxCount) {
                break;
            }
            BatchShape candidate = shape.add(item);
            if (!candidate.fitsCompute(batchMaxTokens)) {
                break;
            }
            if (!candidate.fitsKv(batchKvTokens)) {
                break;
            }

            // SLO check: all items in the expanded batch must still meet their deadline.
            // predicted_batch_time must not exceed (deadline - now) for any item.
            List<BatchItem> trial = new ArrayList<>(picked.size() + 1);
            trial.addAll(picked);
            trial.add(item);
            long trialPredMs = (long) predictor.predictBatchMs(trial);

            boolean sloOk = true;
            for (BatchItem p : trial) {
                if (p.sortKey() - now < trialPredMs) {
                    sloOk = false;
                    break;
                }
            }

            if (sloOk) {
                picked.add(item);
                shape = candidate;
            } else {
                // Adding this outside item would break SLO for some request.
                // Stop adding outside items (subsequent items have even later
                // deadlines but the batch time only grows).
                break;
            }
        }
        return picked;
    }

    private static void dispatch(BatcherContext ctx, List<BatchItem> picked, String reason) {
        BatchItem head = picked.get(0);
        long waitMs = ctx.now() - head.enqueuedAtMs();

        ctx.reporter().reportDispatchReason(RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason);
        ctx.reporter().reportBatchSize(RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason, picked.size());

        // Compute batch-aggregated cache hit ratio
        long totalSeqLen = 0;
        long totalHitCache = 0;
        for (BatchItem item : picked) {
            totalSeqLen += item.seqLen();
            totalHitCache += item.hitCache();
        }
        ctx.reporter().reportBatchCacheHitMetrics(RoleType.PREFILL.name(), ctx.prefillEp().getIp(), totalHitCache, totalSeqLen);
        ctx.reporter().reportBatchTotalTokens(RoleType.PREFILL.name(), ctx.prefillEp().getIp(), reason, totalSeqLen);

        Logger.debug("flexlb_batch_decision reason={} picked_size={} "
                        + "wait_ms={} queue_before={} worker={} head_req_id={} head_priority={}",
                reason, picked.size(), waitMs, ctx.size(), ctx.key(), head.requestId(), head.priority());

        ctx.dispatch(picked,
                new DispatchMeta(reason, ctx.size() - picked.size()));
    }
}
