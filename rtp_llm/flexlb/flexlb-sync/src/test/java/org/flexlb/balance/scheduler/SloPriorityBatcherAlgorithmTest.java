package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.strategy.PrefillTimePredictor;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the v4 SLO-priority batching algorithm.
 *
 * <p>Verifies the sort-key encoding ({@code (MAX_PRIORITY - priority) * SCALE + deadline}),
 * priority-based queue ordering, deadline-based tie-breaking, and the
 * {@code queueWaitMs} estimation logic across various queue states.
 */
@DisplayName("SloPriorityBatcherAlgorithm Tests")
class SloPriorityBatcherAlgorithmTest {

    private static final long SCALE = 10_000_000_000_000L; // 10^13
    private static final int MAX_PRIORITY = 100;

    private SloPriorityBatcherAlgorithm algorithm;

    @BeforeEach
    void setUp() {
        algorithm = new SloPriorityBatcherAlgorithm();
    }

    // ==================== Helpers ====================

    /**
     * Create a BatchItem with a given priority, seqLen, and enqueuedAtMs.
     * The sort key is NOT set here — the caller should compute it via
     * {@link #algorithm}.computeSortKey and set it.
     */
    private static BatchItem enqueuedItem(long requestId, long enqueuedAtMs,
                                          int priority, long seqLen) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setSeqLen(seqLen);
        request.setPriority(priority);
        BalanceContext balanceContext = new BalanceContext();
        balanceContext.setRequest(request);
        balanceContext.setPriority(priority);
        return new BatchItem(balanceContext, null, null, null, null, null, null, enqueuedAtMs);
    }

    private static PriorityBlockingQueue<BatchItem> queueWith(BatchItem... items) {
        PriorityBlockingQueue<BatchItem> queue = new PriorityBlockingQueue<>(
                11, Comparator.comparingLong(BatchItem::sortKey)
                        .thenComparingLong(BatchItem::enqueuedAtMs));
        for (BatchItem item : items) {
            queue.add(item);
        }
        return queue;
    }

    private static BatcherContext context(String key, PrefillEndpoint endpoint,
                                          FlexlbConfig config,
                                          BatchDecisionHandler handler,
                                          PriorityBlockingQueue<BatchItem> queue,
                                          BatchSchedulerReporter reporter) {
        return new BatcherContext(key, endpoint, config, handler, queue,
                new AtomicInteger(queue.size()), reporter);
    }

    private static BatcherContext contextWithNoPredictor(FlexlbConfig config,
                                                          PriorityBlockingQueue<BatchItem> queue) {
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getPredictor()).thenReturn(null);
        return context("test", endpoint, config, null, queue,
                mock(BatchSchedulerReporter.class));
    }

    private static BatcherContext contextWithPredictor(FlexlbConfig config,
                                                        PrefillTimePredictor predictor,
                                                        PriorityBlockingQueue<BatchItem> queue) {
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getPredictor()).thenReturn(predictor);
        return context("test", endpoint, config, null, queue,
                mock(BatchSchedulerReporter.class));
    }

    // ==================== computeSortKey ====================

    @Nested
    @DisplayName("computeSortKey: priority encoding")
    class ComputeSortKeyTests {

        @Test
        @DisplayName("Higher priority produces smaller sortKey (min-heap: smallest = highest priority)")
        void higherPrioritySmallerSortKey() {
            FlexlbConfig config = new FlexlbConfig();
            // Default sloLengthIntervals: seqLen=100 -> baseSlo=150 (0-256 interval)
            // P70 multiplier=0.5 -> sloMs=75; P50 multiplier=1.0 -> sloMs=150
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem highPriority = enqueuedItem(1L, 1000L, 70, 100);
            BatchItem lowPriority = enqueuedItem(2L, 1000L, 50, 100);

            long highKey = algorithm.computeSortKey(ctx, highPriority);
            long lowKey = algorithm.computeSortKey(ctx, lowPriority);

            // P70: sloMs=150*0.5=75, deadline=1000+75=1075
            long expectedHigh = (long) (MAX_PRIORITY - 70) * SCALE + 1075;
            // P50: sloMs=150*1.0=150, deadline=1000+150=1150
            long expectedLow = (long) (MAX_PRIORITY - 50) * SCALE + 1150;

            assertEquals(expectedHigh, highKey);
            assertEquals(expectedLow, lowKey);
            assertTrue(highKey < lowKey, "Higher priority (70) should have smaller sortKey than lower priority (50)");
        }

        @Test
        @DisplayName("Exact sortKey value matches the formula")
        void exactSortKeyValue() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem item = enqueuedItem(42L, 5000L, 60, 2048);

            long sortKey = algorithm.computeSortKey(ctx, item);

            // seqLen=2048 -> baseSlo=600 (1024-4096 interval)
            // P60 multiplier=0.75 -> sloMs=600*0.75=450
            // deadline = 5000 + 450 - 0 = 5450
            // sortKey = (100 - 60) * 10^13 + 5450 = 40 * 10^13 + 5450
            long expected = (long) (MAX_PRIORITY - 60) * SCALE + 5450;
            assertEquals(expected, sortKey);
        }

        @Test
        @DisplayName("Predictor reduces deadline by predMs")
        void predictorReducesDeadline() {
            FlexlbConfig config = new FlexlbConfig();
            PrefillTimePredictor predictor = mock(PrefillTimePredictor.class);
            when(predictor.estimateMs(100L, 0L)).thenReturn(200L);

            BatcherContext ctx = contextWithPredictor(config, predictor,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem item = enqueuedItem(1L, 1000L, 50, 100);

            long sortKey = algorithm.computeSortKey(ctx, item);

            // seqLen=100 -> baseSlo=150, P50 multiplier=1.0 -> sloMs=150
            // predMs = 200, deadline = 1000 + 150 - 200 = 950
            // sortKey = (100 - 50) * 10^13 + 950
            long expected = (long) (MAX_PRIORITY - 50) * SCALE + 950;
            assertEquals(expected, sortKey);
        }
    }

    // ==================== Priority ordering ====================

    @Nested
    @DisplayName("Priority ordering in queue")
    class PriorityOrderingTests {

        @Test
        @DisplayName("Priority 70 is served before priority 50 in the min-heap")
        void higherPriorityServedFirst() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem lowPriority = enqueuedItem(1L, 1000L, 50, 100);
            lowPriority.setSortKey(algorithm.computeSortKey(ctx, lowPriority));

            BatchItem highPriority = enqueuedItem(2L, 1000L, 70, 100);
            highPriority.setSortKey(algorithm.computeSortKey(ctx, highPriority));

            PriorityBlockingQueue<BatchItem> queue = queueWith(lowPriority, highPriority);

            // Higher priority (70) should be at the head of the min-heap
            assertEquals(70, queue.peek().priority());
        }

        @Test
        @DisplayName("Multiple priority levels maintain correct ordering")
        void multiplePriorityLevelsOrdered() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // Add items in reverse priority order
            BatchItem p30 = enqueuedItem(1L, 1000L, 30, 100);
            p30.setSortKey(algorithm.computeSortKey(ctx, p30));
            BatchItem p40 = enqueuedItem(2L, 1000L, 40, 100);
            p40.setSortKey(algorithm.computeSortKey(ctx, p40));
            BatchItem p50 = enqueuedItem(3L, 1000L, 50, 100);
            p50.setSortKey(algorithm.computeSortKey(ctx, p50));
            BatchItem p60 = enqueuedItem(4L, 1000L, 60, 100);
            p60.setSortKey(algorithm.computeSortKey(ctx, p60));
            BatchItem p70 = enqueuedItem(5L, 1000L, 70, 100);
            p70.setSortKey(algorithm.computeSortKey(ctx, p70));

            PriorityBlockingQueue<BatchItem> queue = queueWith(p30, p50, p70, p40, p60);

            // Poll in order — should be 70, 60, 50, 40, 30
            assertEquals(70, queue.poll().priority());
            assertEquals(60, queue.poll().priority());
            assertEquals(50, queue.poll().priority());
            assertEquals(40, queue.poll().priority());
            assertEquals(30, queue.poll().priority());
        }
    }

    // ==================== Same priority, different deadline ====================

    @Nested
    @DisplayName("Same priority, different deadline")
    class DeadlineOrderingTests {

        @Test
        @DisplayName("Earlier deadline is served first when priority is the same")
        void earlierDeadlineServedFirst() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // Same priority, different enqueuedAtMs -> different deadline
            BatchItem earlyDeadline = enqueuedItem(1L, 1000L, 50, 100);
            earlyDeadline.setSortKey(algorithm.computeSortKey(ctx, earlyDeadline));

            BatchItem lateDeadline = enqueuedItem(2L, 2000L, 50, 100);
            lateDeadline.setSortKey(algorithm.computeSortKey(ctx, lateDeadline));

            PriorityBlockingQueue<BatchItem> queue = queueWith(lateDeadline, earlyDeadline);

            // Earlier deadline (enqueuedAtMs=1000) should be at the head
            assertEquals(1L, queue.peek().requestId());
        }

        @Test
        @DisplayName("Deadline ordering holds across different priority bands")
        void deadlineOrderingAcrossBands() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // Priority 70 with late deadline vs priority 50 with early deadline
            // Priority 70 should still come first because (100-70)*10^13 >> any deadline diff
            BatchItem highPriLateDeadline = enqueuedItem(1L, 100000L, 70, 100);
            highPriLateDeadline.setSortKey(algorithm.computeSortKey(ctx, highPriLateDeadline));

            BatchItem lowPriEarlyDeadline = enqueuedItem(2L, 0L, 50, 100);
            lowPriEarlyDeadline.setSortKey(algorithm.computeSortKey(ctx, lowPriEarlyDeadline));

            PriorityBlockingQueue<BatchItem> queue = queueWith(lowPriEarlyDeadline, highPriLateDeadline);

            // Priority 70 should still be first despite later deadline
            assertEquals(70, queue.peek().priority());
        }
    }

    // ==================== queueWaitMs ====================

    @Nested
    @DisplayName("queueWaitMs: wait time estimation")
    class QueueWaitMsTests {

        @Test
        @DisplayName("Empty queue with batchMaxCount > 1 returns fixedWaitMs")
        void emptyQueue_returnsFixedWaitMs() {
            FlexlbConfig config = new FlexlbConfig();
            // Default: fixedWaitMs=300, batchSizeMax=8
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            assertEquals(300, algorithm.queueWaitMs(ctx));
        }

        @Test
        @DisplayName("Empty queue with batchMaxCount <= 1 returns 0")
        void emptyQueue_batchMaxCount1_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchSizeMax(1);
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            assertEquals(0, algorithm.queueWaitMs(ctx));
        }

        @Test
        @DisplayName("Queue with batchMaxCount=1 returns 0 (each request is its own batch)")
        void batchMaxCount1_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchSizeMax(1);
            config.setFlexlbBatchFixedWaitMs(300);

            long now = System.currentTimeMillis();
            BatchItem head = enqueuedItem(1L, now, 50, 100);
            head.setSortKey(now);

            PriorityBlockingQueue<BatchItem> queue = queueWith(head);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            assertEquals(0, algorithm.queueWaitMs(ctx));
        }

        @Test
        @DisplayName("Queue fills the batch (queueSize % batchMaxCount == batchMaxCount - 1) returns 0")
        void queueFillsBatch_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchFixedWaitMs(300);
            config.setFlexlbBatchSizeMax(8);

            long now = System.currentTimeMillis();
            PriorityBlockingQueue<BatchItem> queue = new PriorityBlockingQueue<>(
                    11, Comparator.comparingLong(BatchItem::sortKey));
            // Add 7 items (7 % 8 == 7 == 8-1, so the next arrival fills the batch)
            for (int i = 1; i <= 7; i++) {
                BatchItem item = enqueuedItem(i, now, 50, 100);
                item.setSortKey(now);
                queue.add(item);
            }

            BatcherContext ctx = contextWithNoPredictor(config, queue);

            assertEquals(0, algorithm.queueWaitMs(ctx));
        }

        @Test
        @DisplayName("Queue not full, window timed out returns 0")
        void queueNotFull_windowTimedOut_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchFixedWaitMs(100);
            config.setFlexlbBatchSizeMax(8);

            long pastTime = System.currentTimeMillis() - 500;
            BatchItem head = enqueuedItem(1L, pastTime, 50, 100);
            head.setSortKey(pastTime);

            PriorityBlockingQueue<BatchItem> queue = queueWith(head);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            // elapsedMs ~500 > fixedWaitMs=100
            assertEquals(0, algorithm.queueWaitMs(ctx));
        }

        @Test
        @DisplayName("Queue not full, window not expired returns remaining wait")
        void queueNotFull_windowNotExpired_returnsRemainingWait() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchFixedWaitMs(300);
            config.setFlexlbBatchSizeMax(8);

            long now = System.currentTimeMillis();
            BatchItem head = enqueuedItem(1L, now, 50, 100);
            head.setSortKey(now);

            PriorityBlockingQueue<BatchItem> queue = queueWith(head);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            long waitMs = algorithm.queueWaitMs(ctx);
            // elapsedMs ~0, so waitMs ~ 300 - 0 = 300
            // Allow small timing variance
            assertTrue(waitMs <= 300, "Wait should not exceed fixedWaitMs (300), got " + waitMs);
            assertTrue(waitMs >= 295, "Wait should be close to fixedWaitMs, got " + waitMs);
        }

        @Test
        @DisplayName("Queue depth >= batchMaxCount, doesn't fill the last batch returns fixedWaitMs")
        void queueDepthExceedsBatchMax_returnsFixedWaitMs() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchFixedWaitMs(300);
            config.setFlexlbBatchSizeMax(8);

            long now = System.currentTimeMillis();
            PriorityBlockingQueue<BatchItem> queue = new PriorityBlockingQueue<>(
                    11, Comparator.comparingLong(BatchItem::sortKey)
                            .thenComparingLong(BatchItem::enqueuedAtMs));
            // Add 9 items: 9 % 8 == 1 != 7, queueSize(9) >= batchMaxCount(8)
            for (int i = 1; i <= 9; i++) {
                BatchItem item = enqueuedItem(i, now, 50, 100);
                item.setSortKey(now);
                queue.add(item);
            }

            BatcherContext ctx = contextWithNoPredictor(config, queue);

            // 9 % 8 == 1 != 7, and 9 >= 8, so returns fixedWaitMs
            assertEquals(300, algorithm.queueWaitMs(ctx));
        }
    }

    // ==================== estimateQueueWaitMs ====================

    @Nested
    @DisplayName("estimateQueueWaitMs: SLO-based queue wait estimation")
    class EstimateQueueWaitMsTests {

        @Test
        @DisplayName("Empty queue returns 0 (would be dispatched immediately)")
        void emptyQueue_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            assertEquals(0, algorithm.estimateQueueWaitMs(ctx, 50, 1500, 100, 0.0));
        }

        @Test
        @DisplayName("Incoming is highest priority (no items ahead) returns 0")
        void highestPriority_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();

            BatcherContext computeCtx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            // Queue items: priority 50 (lower priority than incoming)
            BatchItem lowPri1 = enqueuedItem(1L, 1000L, 50, 100);
            lowPri1.setSortKey(algorithm.computeSortKey(computeCtx, lowPri1));
            BatchItem lowPri2 = enqueuedItem(2L, 1000L, 50, 100);
            lowPri2.setSortKey(algorithm.computeSortKey(computeCtx, lowPri2));

            PriorityBlockingQueue<BatchItem> queue = queueWith(lowPri1, lowPri2);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            // Incoming: priority 70 (higher priority -> smaller sortKey -> itemsAhead=0)
            assertEquals(0, algorithm.estimateQueueWaitMs(ctx, 70, 1500, 100, 0.0));
        }

        @Test
        @DisplayName("Incoming is lower priority than queued items returns non-zero wait")
        void lowerPriority_returnsNonZeroWait() {
            FlexlbConfig config = new FlexlbConfig();

            BatcherContext computeCtx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            // Queue: 3 items with priority 70 (higher than incoming's 50)
            BatchItem[] items = new BatchItem[3];
            for (int i = 0; i < 3; i++) {
                items[i] = enqueuedItem(i + 1, 1000L, 70, 100);
                items[i].setSortKey(algorithm.computeSortKey(computeCtx, items[i]));
            }

            PriorityBlockingQueue<BatchItem> queue = queueWith(items);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            // itemsAhead=3, batchSize=8, 3%8=3 != 7 -> not batch_full
            // estimatedWait = ceil(3/8)*300 = 300
            assertEquals(300, algorithm.estimateQueueWaitMs(ctx, 50, 1500, 100, 0.0));
        }

        @Test
        @DisplayName("Queue at batch_full position (size % batchSize == batchSize-1) returns 0")
        void batchFull_returnsZero() {
            FlexlbConfig config = new FlexlbConfig();

            BatcherContext computeCtx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            // Queue: 7 items (7 % 8 == 7 == 8-1 -> batch_full)
            BatchItem[] items = new BatchItem[7];
            for (int i = 0; i < 7; i++) {
                items[i] = enqueuedItem(i + 1, 1000L, 70, 100);
                items[i].setSortKey(algorithm.computeSortKey(computeCtx, items[i]));
            }

            PriorityBlockingQueue<BatchItem> queue = queueWith(items);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            assertEquals(0, algorithm.estimateQueueWaitMs(ctx, 50, 1500, 100, 0.0));
        }

        @Test
        @DisplayName("Large queue with many higher-priority items returns proportionally larger wait")
        void largeQueue_returnsProportionalWait() {
            FlexlbConfig config = new FlexlbConfig();

            BatcherContext computeCtx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            // Queue: 10 items with priority 70 (all ahead of incoming's 50)
            // 10 % 8 == 2 != 7 -> not batch_full
            // itemsAhead=10, estimatedWait = ceil(10/8)*300 = 600
            BatchItem[] items = new BatchItem[10];
            for (int i = 0; i < 10; i++) {
                items[i] = enqueuedItem(i + 1, 1000L, 70, 100);
                items[i].setSortKey(algorithm.computeSortKey(computeCtx, items[i]));
            }

            PriorityBlockingQueue<BatchItem> queue = queueWith(items);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            assertEquals(600, algorithm.estimateQueueWaitMs(ctx, 50, 1500, 100, 0.0));
        }

        @Test
        @DisplayName("Estimate is capped at flexlbBatchEnqueueDeadlineMs")
        void estimateCappedAtDeadlineMs() {
            FlexlbConfig config = new FlexlbConfig();
            config.setFlexlbBatchEnqueueDeadlineMs(500);

            BatcherContext computeCtx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11,
                            Comparator.comparingLong(BatchItem::sortKey)
                                    .thenComparingLong(BatchItem::enqueuedAtMs)));

            // Queue: 20 items (20 % 8 == 4 != 7 -> not batch_full)
            // itemsAhead=20, estimatedWait = ceil(20/8)*300 = 900
            // capped at 500 -> 500
            BatchItem[] items = new BatchItem[20];
            for (int i = 0; i < 20; i++) {
                items[i] = enqueuedItem(i + 1, 1000L, 70, 100);
                items[i].setSortKey(algorithm.computeSortKey(computeCtx, items[i]));
            }

            PriorityBlockingQueue<BatchItem> queue = queueWith(items);
            BatcherContext ctx = contextWithNoPredictor(config, queue);

            assertEquals(500, algorithm.estimateQueueWaitMs(ctx, 50, 1500, 100, 0.0));
        }
    }

    // ==================== Feature 1: Length-based SLO with priority multiplier ====================

    @Nested
    @DisplayName("Length-based SLO intervals")
    class LengthBasedSloTests {

        @Test
        @DisplayName("Different seqLen produces different base SLO (same priority)")
        void differentSeqLenDifferentBaseSlo() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // seqLen=100 -> baseSlo=150, P50 multiplier=1.0 -> sloMs=150
            BatchItem shortReq = enqueuedItem(1L, 1000L, 50, 100);
            long shortDeadline = algorithm.computeSortKey(ctx, shortReq);
            // deadline = 1000 + 150 = 1150
            long expectedShort = (long) (MAX_PRIORITY - 50) * SCALE + 1150;
            assertEquals(expectedShort, shortDeadline);

            // seqLen=500 -> baseSlo=300, P50 multiplier=1.0 -> sloMs=300
            BatchItem midReq = enqueuedItem(2L, 1000L, 50, 500);
            long midDeadline = algorithm.computeSortKey(ctx, midReq);
            // deadline = 1000 + 300 = 1300
            long expectedMid = (long) (MAX_PRIORITY - 50) * SCALE + 1300;
            assertEquals(expectedMid, midDeadline);

            // seqLen=5000 -> baseSlo=1200, P50 multiplier=1.0 -> sloMs=1200
            BatchItem longReq = enqueuedItem(3L, 1000L, 50, 5000);
            long longDeadline = algorithm.computeSortKey(ctx, longReq);
            // deadline = 1000 + 1200 = 2200
            long expectedLong = (long) (MAX_PRIORITY - 50) * SCALE + 2200;
            assertEquals(expectedLong, longDeadline);
        }

        @Test
        @DisplayName("Catch-all interval handles very large seqLen")
        void catchAllIntervalForLargeSeqLen() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // seqLen=100000 -> baseSlo=2400 (catch-all), P50 -> sloMs=2400
            BatchItem hugeReq = enqueuedItem(1L, 1000L, 50, 100000);
            long sortKey = algorithm.computeSortKey(ctx, hugeReq);
            // deadline = 1000 + 2400 = 3400
            long expected = (long) (MAX_PRIORITY - 50) * SCALE + 3400;
            assertEquals(expected, sortKey);
        }

        @Test
        @DisplayName("Interval boundary: seqLen exactly at upper bound uses that interval")
        void intervalBoundaryExact() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // seqLen=256 exactly -> first interval (0-256), baseSlo=150
            BatchItem boundary = enqueuedItem(1L, 1000L, 50, 256);
            long sortKey = algorithm.computeSortKey(ctx, boundary);
            long expected = (long) (MAX_PRIORITY - 50) * SCALE + 1150;
            assertEquals(expected, sortKey);

            // seqLen=257 -> second interval (256-1024), baseSlo=300
            BatchItem over = enqueuedItem(2L, 1000L, 50, 257);
            long sortKeyOver = algorithm.computeSortKey(ctx, over);
            long expectedOver = (long) (MAX_PRIORITY - 50) * SCALE + 1300;
            assertEquals(expectedOver, sortKeyOver);
        }
    }

    @Nested
    @DisplayName("Priority SLO multipliers")
    class PriorityMultiplierTests {

        @Test
        @DisplayName("Same seqLen, different priority produces different SLO")
        void sameSeqLenDifferentPriorityDifferentSlo() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // All seqLen=500 -> baseSlo=300
            // P70: 300*0.5=150, P50: 300*1.0=300, P30: 300*2.0=600
            BatchItem p70 = enqueuedItem(1L, 1000L, 70, 500);
            BatchItem p50 = enqueuedItem(2L, 1000L, 50, 500);
            BatchItem p30 = enqueuedItem(3L, 1000L, 30, 500);

            long key70 = algorithm.computeSortKey(ctx, p70);
            long key50 = algorithm.computeSortKey(ctx, p50);
            long key30 = algorithm.computeSortKey(ctx, p30);

            // P70: deadline=1000+150=1150
            assertEquals((long) (MAX_PRIORITY - 70) * SCALE + 1150, key70);
            // P50: deadline=1000+300=1300
            assertEquals((long) (MAX_PRIORITY - 50) * SCALE + 1300, key50);
            // P30: deadline=1000+600=1600
            assertEquals((long) (MAX_PRIORITY - 30) * SCALE + 1600, key30);
        }

        @Test
        @DisplayName("Unknown priority uses nearest lower priority's multiplier")
        void unknownPriorityUsesNearestLower() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            // Priority 55 is not configured; nearest lower is 50 (multiplier 1.0)
            BatchItem p55 = enqueuedItem(1L, 1000L, 55, 500);
            long key55 = algorithm.computeSortKey(ctx, p55);
            // baseSlo=300, multiplier=1.0, sloMs=300, deadline=1300
            assertEquals((long) (MAX_PRIORITY - 55) * SCALE + 1300, key55);

            // Priority 65 is not configured; nearest lower is 60 (multiplier 0.75)
            BatchItem p65 = enqueuedItem(2L, 1000L, 65, 500);
            long key65 = algorithm.computeSortKey(ctx, p65);
            // baseSlo=300, multiplier=0.75, sloMs=225, deadline=1225
            assertEquals((long) (MAX_PRIORITY - 65) * SCALE + 1225, key65);
        }
    }

    @Nested
    @DisplayName("Combined length + priority SLO")
    class CombinedSloTests {

        @Test
        @DisplayName("P70 500-token SLO = 300ms * 0.5 = 150ms; P30 500-token SLO = 300ms * 2.0 = 600ms")
        void p70ShortVsP30Long() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem p70 = enqueuedItem(1L, 1000L, 70, 500);
            BatchItem p30 = enqueuedItem(2L, 1000L, 30, 500);

            algorithm.computeSortKey(ctx, p70);
            algorithm.computeSortKey(ctx, p30);

            // P70: baseSlo=300, multiplier=0.5, sloMs=150, deadline=1000+150=1150
            assertEquals(1150, p70.deadline());
            // P30: baseSlo=300, multiplier=2.0, sloMs=600, deadline=1000+600=1600
            assertEquals(1600, p30.deadline());
        }

        @Test
        @DisplayName("Deadline is stored on BatchItem after computeSortKey")
        void deadlineStoredOnItem() {
            FlexlbConfig config = new FlexlbConfig();
            BatcherContext ctx = contextWithNoPredictor(config,
                    new PriorityBlockingQueue<>(11, Comparator.comparingLong(BatchItem::sortKey)));

            BatchItem item = enqueuedItem(1L, 5000L, 60, 2048);
            algorithm.computeSortKey(ctx, item);

            // seqLen=2048 -> baseSlo=600, P60 multiplier=0.75 -> sloMs=450
            // deadline = 5000 + 450 = 5450
            assertEquals(5450, item.deadline());
        }
    }

    // ==================== Feature 2: SLO Batcher Request Transfer ====================

    private static class RecordingHandler implements BatchDecisionHandler {
        final List<BatchItem> transferredItems = new ArrayList<>();
        final List<String> transferReasons = new ArrayList<>();

        @Override
        public void onExpired(BatchItem head) { }

        @Override
        public void onBatchReady(List<BatchItem> items, DispatchMeta meta) { }

        @Override
        public void onOfferFailure(BatchItem item, Throwable error) { }

        @Override
        public void onTransferNeeded(BatchItem item, String reason) {
            transferredItems.add(item);
            transferReasons.add(reason);
        }
    }

    private static BatcherContext contextWithHandler(FlexlbConfig config,
                                                      PriorityBlockingQueue<BatchItem> queue,
                                                      BatchDecisionHandler handler) {
        PrefillEndpoint endpoint = mock(PrefillEndpoint.class);
        when(endpoint.getPredictor()).thenReturn(null);
        when(endpoint.getInflightBatchCount()).thenReturn(0);
        return context("test", endpoint, config, handler, queue,
                mock(BatchSchedulerReporter.class));
    }

    @Nested
    @DisplayName("Transfer scan: danger-zone request transfer")
    class TransferScanTests {

        @Test
        @DisplayName("Request in danger zone (deadline close to now) triggers transfer")
        void dangerZoneTriggersTransfer() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);
            config.setSloTransferMinPriority(31);
            config.setSloTransferMaxCount(1);

            long now = System.currentTimeMillis();
            BatchItem item = enqueuedItem(1L, now - 200, 50, 100);
            item.setDeadline(now + 50);
            item.setSortKey(item.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(item);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertEquals(1, handler.transferredItems.size());
            assertEquals(item, handler.transferredItems.get(0));
            assertEquals("danger_zone", handler.transferReasons.get(0));
            assertEquals(1, item.getTransferCount());
            assertTrue(queue.isEmpty());
        }

        @Test
        @DisplayName("Request not in danger zone (deadline far from now) is NOT transferred")
        void notInDangerZoneNotTransferred() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);

            long now = System.currentTimeMillis();
            BatchItem item = enqueuedItem(1L, now, 50, 100);
            item.setDeadline(now + 10000);
            item.setSortKey(item.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(item);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertTrue(handler.transferredItems.isEmpty());
            assertEquals(0, item.getTransferCount());
        }

        @Test
        @DisplayName("Lowest priority (P30) in danger zone is NOT transferred")
        void lowestPriorityNotTransferred() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);
            config.setSloTransferMinPriority(31);

            long now = System.currentTimeMillis();
            BatchItem item = enqueuedItem(1L, now - 200, 30, 100);
            item.setDeadline(now + 50);
            item.setSortKey(item.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(item);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertTrue(handler.transferredItems.isEmpty());
            assertEquals(0, item.getTransferCount());
        }

        @Test
        @DisplayName("Request already transferred (maxCount reached) is NOT transferred again")
        void maxTransfersExceededNotTransferred() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);
            config.setSloTransferMaxCount(1);

            long now = System.currentTimeMillis();
            BatchItem item = enqueuedItem(1L, now - 200, 50, 100);
            item.setDeadline(now + 50);
            item.setSortKey(item.deadline());
            item.setTransferCount(1);

            PriorityBlockingQueue<BatchItem> queue = queueWith(item);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertTrue(handler.transferredItems.isEmpty());
            assertEquals(1, item.getTransferCount());
        }

        @Test
        @DisplayName("Transfer disabled (sloTransferEnabled=false) → no transfers")
        void transferDisabledNoTransfers() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(false);

            long now = System.currentTimeMillis();
            BatchItem item = enqueuedItem(1L, now - 200, 50, 100);
            item.setDeadline(now + 50);
            item.setSortKey(item.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(item);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertTrue(handler.transferredItems.isEmpty());
            assertEquals(0, item.getTransferCount());
        }

        @Test
        @DisplayName("Multiple danger-zone items are all transferred in one scan")
        void multipleDangerZoneItemsAllTransferred() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);
            config.setSloTransferMinPriority(31);
            config.setSloTransferMaxCount(1);

            long now = System.currentTimeMillis();
            BatchItem item1 = enqueuedItem(1L, now - 200, 50, 100);
            item1.setDeadline(now + 50);
            item1.setSortKey(item1.deadline());

            BatchItem item2 = enqueuedItem(2L, now - 200, 70, 200);
            item2.setDeadline(now + 30);
            item2.setSortKey(item2.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(item1, item2);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertEquals(2, handler.transferredItems.size());
            assertTrue(queue.isEmpty());
            assertEquals(1, item1.getTransferCount());
            assertEquals(1, item2.getTransferCount());
        }

        @Test
        @DisplayName("Mixed queue: only danger-zone eligible items transferred, others remain")
        void mixedQueuePartialTransfer() throws Exception {
            FlexlbConfig config = new FlexlbConfig();
            config.setSloTransferEnabled(true);
            config.setSloDangerThresholdMs(100);
            config.setSloTransferMinPriority(31);
            config.setSloTransferMaxCount(1);
            config.setFlexlbBatchFixedWaitMs(60000);

            long now = System.currentTimeMillis();
            // Danger-zone item — should be transferred
            BatchItem danger = enqueuedItem(1L, now - 200, 50, 100);
            danger.setDeadline(now + 50);
            danger.setSortKey(danger.deadline());

            // Safe item — should remain in queue
            BatchItem safe = enqueuedItem(2L, now, 60, 200);
            safe.setDeadline(now + 10000);
            safe.setSortKey(safe.deadline());

            PriorityBlockingQueue<BatchItem> queue = queueWith(danger, safe);
            RecordingHandler handler = new RecordingHandler();
            BatcherContext ctx = contextWithHandler(config, queue, handler);

            algorithm.processQueue(ctx);

            assertEquals(1, handler.transferredItems.size());
            assertEquals(danger, handler.transferredItems.get(0));
            assertEquals(1, queue.size());
            assertTrue(queue.contains(safe));
        }
    }
}
