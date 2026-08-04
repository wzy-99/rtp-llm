package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Logger;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Per-worker request batcher that owns the queue and lifecycle, delegating
 * dispatch decision logic to a pluggable {@link BatcherAlgorithm}.
 *
 * <p>One instance per Prefill worker. Requests are submitted via
 * {@link #offer(BatchItem)} and batched by the configured algorithm.
 */
public class WorkerBatcher {

    private final String key;
    private final FlexlbConfig cfg;
    private final BatchDecisionHandler handler;
    private final PriorityBlockingQueue<BatchItem> queue =
            new PriorityBlockingQueue<>(11,
                    Comparator.comparingLong(BatchItem::sortKey)
                            .thenComparingLong(BatchItem::enqueuedAtMs));
    private final AtomicInteger queueDepth = new AtomicInteger();
    private final Thread workerThread;
    private volatile boolean stopped;
    private final BatcherAlgorithm algorithm;
    private final BatcherContext ctx;

    public WorkerBatcher(String key, PrefillEndpoint prefillEp, FlexlbConfig cfg,
                         BatchDecisionHandler handler,
                         BatchSchedulerReporter reporter) {
        this.key = key;
        this.cfg = cfg;
        this.handler = handler;
        this.algorithm = createAlgorithm(cfg);
        this.ctx = new BatcherContext(
                key, prefillEp, cfg, handler, queue, queueDepth, reporter);
        this.workerThread = new Thread(this::runLoop, "flexlb-batcher-" + key);
        this.workerThread.setDaemon(true);
        this.workerThread.setUncaughtExceptionHandler((t, e) ->
                Logger.error("WorkerBatcher[{}] thread died unexpectedly", key, e));
    }

    private static BatcherAlgorithm createAlgorithm(FlexlbConfig config) {
        String algoName = config.getFlexlbBatchAlgorithm();
        if ("fixed_window".equalsIgnoreCase(algoName)) {
            return new FixedWindowBatcherAlgorithm();
        }
        if ("slo_priority".equalsIgnoreCase(algoName)) {
            return new SloPriorityBatcherAlgorithm();
        }
        // Fallback: slo_budget for any unrecognized value
        return new SloBudgetBatcherAlgorithm();
    }

    public void start() {
        workerThread.start();
    }

    public void offer(BatchItem item) {
        if (stopped) {
            handler.onOfferFailure(item, new IllegalStateException("FlexLB batcher stopped"));
            return;
        }
        int maxSize = cfg.getFlexlbBatchQueueMaxSize();
        if (!reserveQueueSlot(maxSize)) {
            handler.onOfferFailure(item,
                    new IllegalStateException("FlexLB batcher queue full, maxSize=" + maxSize));
            return;
        }
        try {
            long sortKey = algorithm.computeSortKey(ctx, item);
            item.setSortKey(sortKey);
            algorithm.onOffer(ctx, item, System.currentTimeMillis());
            queue.add(item);
        } catch (RuntimeException | Error e) {
            queueDepth.decrementAndGet();
            throw e;
        }
    }

    public int queueSize() {
        return queueDepth.get();
    }

    /**
     * Estimated time a new request would wait in the queue before dispatch.
     * Delegates to the algorithm-specific {@link BatcherAlgorithm#queueWaitMs}.
     */
    public long queueWaitMs() {
        return algorithm.queueWaitMs(ctx);
    }

    /**
     * Estimate how long an incoming request would wait in the priority queue
     * before being dispatched, using the SLO-priority algorithm's queue-depth
     * estimation. Falls back to {@link #queueWaitMs()} for non-SLO algorithms.
     *
     * <p>Called by the strategy layer to make SLO-based eviction decisions.
     *
     * @param incomingPriority   priority of the incoming request
     * @param incomingDeadlineMs deadline of the incoming request
     *                           (= {@code now + sloMs - predMs})
     * @param incomingSeqLen     sequence length of the incoming request
     * @param incomingCacheHit    cache hit ratio of the incoming request
     * @return estimated wait time in milliseconds, or 0 if dispatched immediately
     */
    public long estimateQueueWaitMs(int incomingPriority, long incomingDeadlineMs,
                                    int incomingSeqLen, double incomingCacheHit) {
        if (algorithm instanceof SloPriorityBatcherAlgorithm sloAlgo) {
            return sloAlgo.estimateQueueWaitMs(ctx, incomingPriority,
                    incomingDeadlineMs, incomingSeqLen, incomingCacheHit);
        }
        return queueWaitMs();
    }

    public void shutdown() {
        stopped = true;
        workerThread.interrupt();
        algorithm.onShutdown(ctx);
        List<BatchItem> remaining = new ArrayList<>();
        ctx.drainTo(remaining);
        for (BatchItem item : remaining) {
            handler.onOfferFailure(item,
                    new CancellationException("FlexLB batcher stopped: " + key));
        }
    }


    // ==================== Priority Eviction (Stage 2) ====================

    /**
     * Find evictable queued requests with priority strictly less than incomingPriority.
     *
     * <p>Traverses the batcher PQ, collects {@link BatchItem}s with
     * {@code priority < incomingPriority}, sorts by:
     * <ol>
     *   <li>Priority lowest first (lowest priority = most evictable)</li>
     *   <li>SLO slack largest first (sortKey descending = more time remaining = more evictable)</li>
     * </ol>
     * Returns up to {@code needEvictCount} victims. Does NOT remove from queue —
     * caller must call {@link #evictQueuedItems} to actually remove.
     *
     * @param incomingPriority the incoming request's priority (victims must be strictly lower)
     * @param needEvictCount   maximum number of victims to return
     * @return list of evictable BatchItems (may be empty)
     */
    public List<BatchItem> findEvictableQueuedRequests(int incomingPriority, int needEvictCount) {
        if (needEvictCount <= 0 || queue.isEmpty()) {
            return List.of();
        }
        List<BatchItem> candidates = new ArrayList<>();
        for (BatchItem item : queue) {
            if (item.priority() < incomingPriority) {
                candidates.add(item);
            }
        }
        if (candidates.isEmpty()) {
            return List.of();
        }
        candidates.sort(Comparator
                .comparingInt(BatchItem::priority)
                .thenComparingLong(BatchItem::sortKey).reversed()); // sortKey desc = more slack
        int limit = Math.min(needEvictCount, candidates.size());
        return candidates.subList(0, limit);
    }

    /**
     * Remove evicted BatchItems from the PQ and notify the handler for cleanup.
     *
     * <p>For each victim:
     * <ol>
     *   <li>Remove from PQ ({@code queue.remove})</li>
     *   <li>Decrement queueDepth</li>
     *   <li>Call {@code handler.onOfferFailure} with PRIORITY_PREEMPTED cancellation
     *       — this triggers future completion, decode rollback, and inflight removal
     *       in {@link FlexlbBatchScheduler}</li>
     * </ol>
     * No engine cancel (victims are pre-dispatch queue items).
     *
     * @param victims the BatchItems to evict (must be from this batcher's queue)
     */
    public void evictQueuedItems(List<BatchItem> victims) {
        for (BatchItem victim : victims) {
            boolean removed = queue.remove(victim);
            if (removed) {
                queueDepth.decrementAndGet();
                handler.onOfferFailure(victim,
                        new CancellationException("PRIORITY_PREEMPTED"));
            }
        }
    }

    // ==================== Internal: Run loop ====================

    private void runLoop() {
        while (!stopped && !Thread.currentThread().isInterrupted()) {
            try {
                waitForNonEmpty();
                algorithm.processQueue(ctx);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                return;
            } catch (Throwable t) {
                Logger.error("WorkerBatcher[{}] loop failed", key, t);
            }
        }
    }

    private void waitForNonEmpty() throws InterruptedException {
        BatchItem item = queue.take();
        queue.put(item);
    }

    private boolean reserveQueueSlot(int maxSize) {
        if (maxSize <= 0) {
            queueDepth.incrementAndGet();
            return true;
        }
        while (true) {
            int current = queueDepth.get();
            if (current >= maxSize) {
                return false;
            }
            if (queueDepth.compareAndSet(current, current + 1)) {
                return true;
            }
        }
    }
}
