package org.flexlb.balance.scheduler;

import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.enums.ScheduleModeEnum;
import org.flexlb.service.monitor.RoutingQueueReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class QueueSchedulerTest {

    @Mock
    private RoutingQueueReporter metrics;
    @Mock
    private ConfigService configService;

    private QueueScheduler queueManager;

    @BeforeEach
    void setUp() {
        FlexlbConfig config = new FlexlbConfig();
        config.setMaxQueueSize(10);
        when(configService.loadBalanceConfig()).thenReturn(config);
        queueManager = new QueueScheduler(metrics, configService);
    }

    @Test
    void dispatch_shouldEnqueueSuccessfully() {
        FlexlbRequest request = createRequest(1L);
        var mono = queueManager.dispatch(request);

        assertNotNull(mono);
        assertNotNull(request.getFuture());
        verify(metrics).reportQueueEntry();
    }

    @Test
    void dispatch_shouldRejectWhenQueueFull() {
        // Fill the queue
        for (int i = 0; i < 10; i++) {
            queueManager.dispatch(createRequest(i));
        }

        // 11th request should be rejected
        FlexlbRequest request = createRequest(11L);
        Response response = queueManager.dispatch(request).block();

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals(StrategyErrorType.QUEUE_FULL.getErrorCode(), response.getCode());
        verify(metrics).reportRejected();
    }

    @Test
    void takeRequest_shouldReturnNullWhenEmpty() {
        QueueScheduler.QueueSlot result = queueManager.takeRequest(false, 0);
        assertNull(result);
    }

    @Test
    void takeRequest_shouldReturnEnqueuedRequest() {
        FlexlbRequest request = createRequest(1L);
        queueManager.dispatch(request);

        QueueScheduler.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(1L, taken.getRequest().getRequestId());
    }

    @Test
    void takeRequest_shouldSkipCancelledRequests() {
        FlexlbRequest cancelled = createRequest(1L);
        queueManager.dispatch(cancelled);
        cancelled.cancel();

        FlexlbRequest valid = createRequest(2L);
        queueManager.dispatch(valid);

        QueueScheduler.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(2L, taken.getRequest().getRequestId());
    }

    @Test
    void offerToHead_shouldRequeueAtFront() {
        FlexlbRequest first = createRequest(1L);
        queueManager.dispatch(first);

        FlexlbRequest retried = createRequest(2L);
        QueueScheduler.QueueSlot slot = new QueueScheduler.QueueSlot(retried, System.currentTimeMillis(), 0L);
        queueManager.offerToHead(slot);

        QueueScheduler.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(2L, taken.getRequest().getRequestId());
    }

    @Test
    void offerToHead_shouldCompleteWithErrorWhenQueueFull() {
        // Fill the queue
        for (int i = 0; i < 10; i++) {
            queueManager.dispatch(createRequest(i));
        }

        FlexlbRequest request = createRequest(99L);
        QueueScheduler.QueueSlot slot = new QueueScheduler.QueueSlot(request, System.currentTimeMillis(), 99L);

        queueManager.offerToHead(slot);

        assertTrue(request.getFuture().isDone());
        Response response = request.getFuture().join();
        assertFalse(response.isSuccess());
        assertEquals(StrategyErrorType.QUEUE_FULL.getErrorCode(), response.getCode());
    }

    @Test
    void cancel_shouldCancelQueuedRequest() {
        FlexlbRequest request = createRequest(1L);
        queueManager.dispatch(request);

        boolean cancelled = queueManager.cancel(1L);
        assertTrue(cancelled);
        assertTrue(request.isCancelled());
        assertTrue(request.getFuture().isCompletedExceptionally());
    }

    @Test
    void cancel_returnsFalseForUnknownRequest() {
        boolean cancelled = queueManager.cancel(999L);
        assertFalse(cancelled);
    }

    // ---- shouldHandle mode dispatch ----

    @Test
    void shouldHandle_returnsTrueForAutoWhenQueueingEnabled() {
        FlexlbConfig config = new FlexlbConfig();
        config.setEnableQueueing(true);
        FlexlbRequest request = createRequest(1L, ScheduleModeEnum.AUTO);
        assertTrue(queueManager.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_returnsFalseForAutoWhenQueueingDisabled() {
        FlexlbConfig config = new FlexlbConfig();
        config.setEnableQueueing(false);
        FlexlbRequest request = createRequest(2L, ScheduleModeEnum.AUTO);
        assertFalse(queueManager.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_returnsFalseForDirectEvenWhenQueueingEnabled() {
        // Bug fix: DIRECT mode must bypass QueueScheduler even if queueing is enabled
        FlexlbConfig config = new FlexlbConfig();
        config.setEnableQueueing(true);
        FlexlbRequest request = createRequest(3L, ScheduleModeEnum.DIRECT);
        assertFalse(queueManager.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_returnsFalseForBatchEvenWhenQueueingEnabled() {
        FlexlbConfig config = new FlexlbConfig();
        config.setEnableQueueing(true);
        FlexlbRequest request = createRequest(4L, ScheduleModeEnum.BATCH);
        assertFalse(queueManager.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_returnsFalseWhenConfigIsNull() {
        FlexlbRequest request = createRequest(5L, ScheduleModeEnum.AUTO);
        assertFalse(queueManager.shouldHandle(request, null));
    }

    private FlexlbRequest createRequest(long requestId) {
        return createRequest(requestId, ScheduleModeEnum.AUTO);
    }

    private FlexlbRequest createRequest(long requestId, ScheduleModeEnum mode) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setGenerateTimeout(60_000);
        FlexlbRequest flexlbRequest = new FlexlbRequest(request);
        flexlbRequest.setScheduleMode(mode);
        return flexlbRequest;
    }
}
