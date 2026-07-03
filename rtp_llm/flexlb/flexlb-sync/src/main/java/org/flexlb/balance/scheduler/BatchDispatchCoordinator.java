package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.strategy.PrefillTimePredictor;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.route.RoleType;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Implements {@link BatchDecisionHandler} — receives batch-assembly decisions
 * from {@link WorkerBatcher} and coordinates gRPC dispatch.
 *
 * <p>Responsibilities:
 * <ul>
 *   <li>{@code onExpired} — SLO-deadline expiry, delegate to {@link CleanupCoordinator}</li>
 *   <li>{@code onBatchReady} — commit batch to PrefillEndpoint, delegate to {@link BatchDispatcher}</li>
 *   <li>{@code onOfferFailure} — batcher queue full, delegate to {@link CleanupCoordinator}</li>
 * </ul>
 */
@Component
public class BatchDispatchCoordinator implements BatchDecisionHandler {

    private final BatchDispatcher dispatcher;
    private final BatchInflightStore store;
    private final CleanupCoordinator cleanupCoordinator;
    private final BatchSchedulerReporter reporter;
    private final DispatchResultHandler dispatchResultHandler;
    private final AtomicLong batchIdGenerator = new AtomicLong(0);

    public BatchDispatchCoordinator(BatchDispatcher dispatcher,
                                    BatchInflightStore store,
                                    CleanupCoordinator cleanupCoordinator,
                                    BatchSchedulerReporter reporter,
                                    DispatchResultHandler dispatchResultHandler) {
        this.dispatcher = dispatcher;
        this.store = store;
        this.cleanupCoordinator = cleanupCoordinator;
        this.reporter = reporter;
        this.dispatchResultHandler = dispatchResultHandler;
    }

    @Override
    public void onExpired(BatchItem head) {
        cleanupCoordinator.terminate(store, head, TerminationReason.SLO_EXPIRED);
    }

    @Override
    public void onBatchReady(List<BatchItem> items, DispatchMeta meta) {
        flushItems(items, meta.reason());
    }

    @Override
    public void onOfferFailure(BatchItem item, Throwable error) {
        cleanupCoordinator.terminate(store, item, TerminationReason.OFFER_FAILED,
                "Batcher offer failed: " + error.getMessage());
    }

    // ==================== Dispatch pipeline ====================

    /**
     * Commit batch to PrefillEndpoint, filter cancelled items, then delegate
     * to {@link BatchDispatcher} for asynchronous gRPC dispatch.
     */
    private void flushItems(List<BatchItem> items, String reason) {
        PrefillEndpoint prefillEp = items.get(0).prefillEp();

        // [SYNC] Filter cancelled/done items first
        List<BatchItem> active = items.stream()
                .filter(item -> !item.isCancelled() && !item.future().isDone())
                .toList();

        // Terminate items that were cancelled before dispatch
        for (BatchItem item : items) {
            if (!active.contains(item)) {
                cleanupCoordinator.terminate(store, item, TerminationReason.CANCELLED);
            }
        }

        if (active.isEmpty()) {
            return;
        }

        // [SYNC] Compute prediction and commit only active items to endpoint
        long predMs = 0;
        long batchId = batchIdGenerator.incrementAndGet();
        if (prefillEp != null) {
            PrefillTimePredictor predictor = prefillEp.getPredictor();
            predMs = predictor.predictBatchMs(active);
            Map<Integer, List<BatchItem>> byDpRank = new LinkedHashMap<>();
            for (BatchItem item : active) {
                byDpRank.computeIfAbsent((int) item.prefill().getDpRank(), k -> new ArrayList<>()).add(item);
            }
            prefillEp.commitBatch(batchId, predMs, byDpRank);
        }

        // Store batchId in inflight items so cancel() can repack the batch.
        for (BatchItem item : active) {
            BatchItem inflightItem = store.get(item.requestId());
            if (inflightItem != null) {
                inflightItem.setBatchId(batchId);
            }
        }

        // [ASYNC] Delegate gRPC dispatch — dispatcher owns its own thread pool
        long waitMs = System.currentTimeMillis() - items.get(0).enqueuedAtMs();
        reporter.reportBatchWaitTimeMs(RoleType.PREFILL.name(), prefillEp != null ? prefillEp.getIp() : "", waitMs);
        dispatcher.dispatch(active, prefillEp, batchId, predMs, reason, dispatchResultHandler);
    }
}
