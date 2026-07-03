package org.flexlb.balance.scheduler;

import lombok.Getter;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.QueueSnapshot;
import org.flexlb.dao.loadbalance.QueueSnapshotResponse;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.service.monitor.RoutingQueueReporter;
import org.flexlb.util.JsonUtils;
import org.flexlb.util.Logger;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingDeque;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Request queue manager
 *
 * @author saichen.sm
 * @since 2025/12/22
 */
@Getter
@Component
public class QueueManager {

    private static final String SNAPSHOT_DIR = "/tmp/flexlb-queue-snapshots";
    private static final int MAX_SNAPSHOT_FILES = 10;

    private final RoutingQueueReporter metrics;

    private final AtomicLong sequenceGenerator = new AtomicLong(0);

    // Request queue — QueueSlot wraps FlexlbRequest and tracks Queue-specific state
    private final BlockingDeque<QueueSlot> queue;

    // Request index for O(1) cancel lookup
    private final ConcurrentHashMap<Long, FlexlbRequest> requestIndex = new ConcurrentHashMap<>();

    public QueueManager(RoutingQueueReporter routingQueueReporter, ConfigService configService) {
        this.metrics = routingQueueReporter;
        this.queue = new LinkedBlockingDeque<>(configService.loadBalanceConfig().getMaxQueueSize());
    }

    /**
     * Attempt to route request
     * <p>
     * Queue and wait asynchronously if resources are insufficient
     *
     * @param request FlexLB request
     * @return Routing result
     */
    public Mono<Response> tryRouteAsync(FlexlbRequest request) {
        // Add to queue tail
        QueueSlot slot = new QueueSlot(request, System.currentTimeMillis(), sequenceGenerator.incrementAndGet());
        boolean added = queue.offerLast(slot);
        if (!added) {
            Logger.warn("Queue is full for request id: {}, current size: {}", request.getRequestId(), queue.size());
            metrics.reportRejected();
            return Mono.just(Response.error(StrategyErrorType.QUEUE_FULL));
        }
        metrics.reportQueueEntry();
        requestIndex.put(request.getRequestId(), request);

        return Mono.fromFuture(request.getFuture())
                .timeout(Duration.ofMillis(request.getRequest().getGenerateTimeout()))
                .onErrorResume(e -> handleQueueException(slot, e))
                .doFinally(signalType -> {
                    requestIndex.remove(request.getRequestId());
                    if (slot.getDequeueTime() > 0) {
                        long routeExecutionTimeMs = System.currentTimeMillis() - slot.getDequeueTime();
                        metrics.reportRouteExecutionMetric(routeExecutionTimeMs);
                    }
                });
    }

    /**
     * Cancel a request by ID.
     * <p>Removes the request from the queue index and the queue itself,
     * sets the cancelled flag, and completes the future exceptionally.
     *
     * @param requestId the request ID to cancel
     * @return true if the request was found and cancelled
     */
    public boolean cancelByRequestId(long requestId) {
        FlexlbRequest req = requestIndex.remove(requestId);
        if (req == null) {
            return false;
        }
        queue.removeIf(slot -> slot.getRequest().getRequestId() == requestId);
        req.cancel();
        req.getFuture().completeExceptionally(new CancellationException("Request cancelled by client"));
        return true;
    }

    /**
     * Offer to queue head (for retry on failure)
     *
     * @param slot Queue slot to re-queue
     */
    public void offerToHead(QueueSlot slot) {
        boolean added = queue.offerFirst(slot);
        if (!added) {
            Logger.warn("Failed to re-queue request id: {} (queue full), completing with error", slot.getRequest().getRequestId());
            slot.getRequest().getFuture().complete(Response.error(StrategyErrorType.QUEUE_FULL));
        }
    }

    /**
     * Take request from queue (blocking/non-blocking)
     *
     * @param isBlock          Whether to block and wait
     * @param blockTimeoutMs   Block timeout in milliseconds
     * @return Queue slot, null if queue is empty
     */
    public QueueSlot takeRequest(boolean isBlock, long blockTimeoutMs) {
        return takeValidRequest(queue, isBlock, blockTimeoutMs);
    }

    /**
     * Take a single valid request from queue
     * <p>
     * Checks for cancelled and timed-out requests, completes future for invalid requests
     *
     * @param sourceQueue Source queue
     * @return Queue slot, null if queue is empty
     */
    private QueueSlot takeValidRequest(BlockingQueue<QueueSlot> sourceQueue, boolean isBlock, long blockTimeoutMs) {
        try {
            while (true) {
                QueueSlot slot = isBlock ? sourceQueue.poll(blockTimeoutMs, TimeUnit.MILLISECONDS) : sourceQueue.poll();
                if (slot == null) {
                    return null;
                }
                slot.setDequeueTime(System.currentTimeMillis());
                FlexlbRequest request = slot.getRequest();
                if (request.isCancelled()) {
                    request.getFuture().completeExceptionally(new CancellationException("Request cancelled by client"));
                    continue;
                }
                long waitTimeMs = System.currentTimeMillis() - slot.getEnqueueTime();
                long maxQueueWaitTimeMs = request.getRequest().getGenerateTimeout();
                if (waitTimeMs > maxQueueWaitTimeMs) {
                    request.getFuture().completeExceptionally(new TimeoutException("Request timeout in queue"));
                    continue;
                }
                long queueWaitTimeMs = slot.getDequeueTime() - slot.getEnqueueTime();
                metrics.reportQueueWaitingMetric(queueWaitTimeMs);
                return slot;
            }
        } catch (Exception e) {
            Logger.error("Failed to take request from queue", e);
            return null;
        }
    }

    private void handleTimeout(QueueSlot slot) {
        remove(slot);
        metrics.reportTimeout();

        long waitTimeMs = System.currentTimeMillis() - slot.getEnqueueTime();
        Logger.warn("Request timeout in queue for id: {}, wait time: {}ms", slot.getRequest().getRequestId(), waitTimeMs);
    }

    private void handleCanceled(QueueSlot slot) {
        remove(slot);
        metrics.reportCancelled();

        long waitTimeMs = System.currentTimeMillis() - slot.getEnqueueTime();
        Logger.warn("Request canceled in queue for id: {}, wait time: {}ms", slot.getRequest().getRequestId(), waitTimeMs);
    }

    private void handleInterruption(QueueSlot slot) {
        remove(slot);
        Thread.currentThread().interrupt();
        Logger.error("Request interrupted while waiting in queue for id: {}", slot.getRequest().getRequestId());
    }

    private void remove(QueueSlot slot) {
        boolean removed = queue.remove(slot);
        if (!removed) {
            Logger.error("Failed to remove timeout request from queue:{}", slot.getRequest().getRequestId());
        }
    }

    private Mono<Response> handleQueueException(QueueSlot slot, Throwable e) {
        // Handle ExecutionException wrapper (consistent with synchronous version)
        Throwable cause = e instanceof ExecutionException ? e.getCause() : e;
        if (cause instanceof TimeoutException) {
            handleTimeout(slot);
            return Mono.just(Response.error(StrategyErrorType.QUEUE_TIMEOUT));
        } else if (cause instanceof CancellationException) {
            handleCanceled(slot);
            return Mono.just(Response.error(StrategyErrorType.REQUEST_CANCELLED));
        } else if (cause instanceof InterruptedException) {
            handleInterruption(slot);
            return Mono.just(Response.error(StrategyErrorType.QUEUE_TIMEOUT));
        }
        // Other exceptions: log and return NO_AVAILABLE_WORKER (consistent with synchronous version)
        Logger.error("Request execution failed error: {}", e);
        return Mono.just(Response.error(StrategyErrorType.NO_AVAILABLE_WORKER));
    }

    @Scheduled(fixedRate = 1000)
    public void reportQueueSize() {
        metrics.reportQueueSize(queue.size());
    }

    public QueueSnapshotResponse snapshotQueue() {
        List<QueueSnapshot> snapshots = new ArrayList<>();
        long currentTime = System.currentTimeMillis();

        for (QueueSlot slot : queue.toArray(new QueueSlot[0])) {
            QueueSnapshot snapshot = new QueueSnapshot();
            snapshot.setSequenceId(slot.getSequenceId());
            snapshot.setRequestId(slot.getRequest().getRequestId());
            snapshot.setEnqueueTime(slot.getEnqueueTime());
            snapshot.setWaitTimeMs(currentTime - slot.getEnqueueTime());
            snapshot.setRetryCount(slot.getRetryCount().get());
            snapshot.setQueueType("main");
            snapshots.add(snapshot);
        }

        try {
            Path dirPath = Paths.get(SNAPSHOT_DIR);
            if (!Files.exists(dirPath)) {
                Files.createDirectories(dirPath);
            }

            // Clean up old snapshots, keep at most MAX_SNAPSHOT_FILES
            cleanOldSnapshots(dirPath);

            long timestamp = System.currentTimeMillis();
            String fileName = "queue-snapshot-" + timestamp + ".json";
            Path filePath = dirPath.resolve(fileName);

            String jsonContent = JsonUtils.toFormattedString(snapshots);
            Files.writeString(filePath, jsonContent);

            QueueSnapshotResponse response = new QueueSnapshotResponse();
            response.setFilePath(filePath.toAbsolutePath().toString());
            response.setTimestamp(timestamp);
            response.setCount(snapshots.size());

            return response;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create queue snapshot", e);
        }
    }

    private void cleanOldSnapshots(Path dirPath) {
        try {
            List<Path> snapshotFiles = Files.list(dirPath)
                    .filter(p -> p.getFileName().toString().startsWith("queue-snapshot-"))
                    .sorted()
                    .collect(java.util.stream.Collectors.toList());
            // Keep at most MAX_SNAPSHOT_FILES - 1 so the new one makes it MAX_SNAPSHOT_FILES
            while (snapshotFiles.size() >= MAX_SNAPSHOT_FILES) {
                Files.deleteIfExists(snapshotFiles.remove(0));
            }
        } catch (IOException e) {
            Logger.warn("Failed to clean old queue snapshots: {}", e.getMessage());
        }
    }

    /**
     * Queue-specific wrapper around FlexlbRequest.
     * Tracks enqueue/dequeue time, sequence id, and retry count —
     * fields that are Queue-path-specific and not stored in FlexlbRequest.
     */
    public static final class QueueSlot {
        private final FlexlbRequest request;
        private final long enqueueTime;
        private final long sequenceId;
        private final AtomicInteger retryCount = new AtomicInteger(0);
        private volatile long dequeueTime;

        public QueueSlot(FlexlbRequest request, long enqueueTime, long sequenceId) {
            this.request = request;
            this.enqueueTime = enqueueTime;
            this.sequenceId = sequenceId;
        }

        public FlexlbRequest getRequest() { return request; }
        public long getEnqueueTime() { return enqueueTime; }
        public long getSequenceId() { return sequenceId; }
        public AtomicInteger getRetryCount() { return retryCount; }
        public long getDequeueTime() { return dequeueTime; }
        public void setDequeueTime(long dequeueTime) { this.dequeueTime = dequeueTime; }

        public int incrementRetryCount() { return retryCount.incrementAndGet(); }
    }
}
