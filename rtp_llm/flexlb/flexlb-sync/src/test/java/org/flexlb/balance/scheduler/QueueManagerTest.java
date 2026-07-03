package org.flexlb.balance.scheduler;

import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
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
class QueueManagerTest {

    @Mock
    private RoutingQueueReporter metrics;
    @Mock
    private ConfigService configService;

    private QueueManager queueManager;

    @BeforeEach
    void setUp() {
        FlexlbConfig config = new FlexlbConfig();
        config.setMaxQueueSize(10);
        when(configService.loadBalanceConfig()).thenReturn(config);
        queueManager = new QueueManager(metrics, configService);
    }

    @Test
    void tryRouteAsync_shouldEnqueueSuccessfully() {
        FlexlbRequest request = createRequest(1L);
        var mono = queueManager.tryRouteAsync(request);

        assertNotNull(mono);
        assertNotNull(request.getFuture());
        verify(metrics).reportQueueEntry();
    }

    @Test
    void tryRouteAsync_shouldRejectWhenQueueFull() {
        // Fill the queue
        for (int i = 0; i < 10; i++) {
            queueManager.tryRouteAsync(createRequest(i));
        }

        // 11th request should be rejected
        FlexlbRequest request = createRequest(11L);
        Response response = queueManager.tryRouteAsync(request).block();

        assertNotNull(response);
        assertFalse(response.isSuccess());
        assertEquals(StrategyErrorType.QUEUE_FULL.getErrorCode(), response.getCode());
        verify(metrics).reportRejected();
    }

    @Test
    void takeRequest_shouldReturnNullWhenEmpty() {
        QueueManager.QueueSlot result = queueManager.takeRequest(false, 0);
        assertNull(result);
    }

    @Test
    void takeRequest_shouldReturnEnqueuedRequest() {
        FlexlbRequest request = createRequest(1L);
        queueManager.tryRouteAsync(request);

        QueueManager.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(1L, taken.getRequest().getRequestId());
    }

    @Test
    void takeRequest_shouldSkipCancelledRequests() {
        FlexlbRequest cancelled = createRequest(1L);
        queueManager.tryRouteAsync(cancelled);
        cancelled.cancel();

        FlexlbRequest valid = createRequest(2L);
        queueManager.tryRouteAsync(valid);

        QueueManager.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(2L, taken.getRequest().getRequestId());
    }

    @Test
    void offerToHead_shouldRequeueAtFront() {
        FlexlbRequest first = createRequest(1L);
        queueManager.tryRouteAsync(first);

        FlexlbRequest retried = createRequest(2L);
        QueueManager.QueueSlot slot = new QueueManager.QueueSlot(retried, System.currentTimeMillis(), 0L);
        queueManager.offerToHead(slot);

        QueueManager.QueueSlot taken = queueManager.takeRequest(false, 0);
        assertNotNull(taken);
        assertEquals(2L, taken.getRequest().getRequestId());
    }

    @Test
    void offerToHead_shouldCompleteWithErrorWhenQueueFull() {
        // Fill the queue
        for (int i = 0; i < 10; i++) {
            queueManager.tryRouteAsync(createRequest(i));
        }

        FlexlbRequest request = createRequest(99L);
        QueueManager.QueueSlot slot = new QueueManager.QueueSlot(request, System.currentTimeMillis(), 99L);

        queueManager.offerToHead(slot);

        assertTrue(request.getFuture().isDone());
        Response response = request.getFuture().join();
        assertFalse(response.isSuccess());
        assertEquals(StrategyErrorType.QUEUE_FULL.getErrorCode(), response.getCode());
    }

    @Test
    void cancelByRequestId_shouldCancelQueuedRequest() {
        FlexlbRequest request = createRequest(1L);
        queueManager.tryRouteAsync(request);

        boolean cancelled = queueManager.cancelByRequestId(1L);
        assertTrue(cancelled);
        assertTrue(request.isCancelled());
        assertTrue(request.getFuture().isCompletedExceptionally());
    }

    @Test
    void cancelByRequestId_returnsFalseForUnknownRequest() {
        boolean cancelled = queueManager.cancelByRequestId(999L);
        assertFalse(cancelled);
    }

    private FlexlbRequest createRequest(long requestId) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setGenerateTimeout(60_000);
        return new FlexlbRequest(request);
    }
}
