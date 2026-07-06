package org.flexlb.service;

import org.flexlb.balance.scheduler.AbstractScheduler;
import org.flexlb.balance.scheduler.BatchScheduler;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.enums.ScheduleModeEnum;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import reactor.core.publisher.Mono;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RouteServiceTest {

    @Mock
    private ConfigService configService;
    @Mock
    private RecentCacheKeyTraceReporter recentCacheKeyTraceReporter;
    @Mock
    private AbstractScheduler batchChannel;
    @Mock
    private AbstractScheduler queueChannel;
    @Mock
    private AbstractScheduler directChannel;

    private RouteService routeService;
    private FlexlbConfig config;

    @BeforeEach
    void setUp() {
        config = new FlexlbConfig();
        lenient().when(configService.loadBalanceConfig()).thenReturn(config);

        lenient().when(batchChannel.getOrder()).thenReturn(30);
        lenient().when(queueChannel.getOrder()).thenReturn(20);
        lenient().when(directChannel.getOrder()).thenReturn(10);

        routeService = new RouteService(configService,
                List.of(batchChannel, queueChannel, directChannel),
                recentCacheKeyTraceReporter);
    }

    // ---- route() path selection ----

    @Test
    void route_usesDirectChannelWhenBatchAndQueueDecline() {
        when(batchChannel.shouldHandle(any(), any())).thenReturn(false);
        when(queueChannel.shouldHandle(any(), any())).thenReturn(false);
        when(directChannel.shouldHandle(any(), any())).thenReturn(true);

        Response success = new Response();
        success.setSuccess(true);
        when(directChannel.dispatch(any())).thenReturn(Mono.just(success));

        FlexlbRequest request = createRequest(1L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(directChannel).dispatch(request);
        verify(batchChannel, never()).dispatch(any());
        verify(queueChannel, never()).dispatch(any());
        verify(recentCacheKeyTraceReporter).report(request);
    }

    @Test
    void route_usesQueueChannelWhenBatchDeclinesAndQueueAccepts() {
        when(batchChannel.shouldHandle(any(), any())).thenReturn(false);
        when(queueChannel.shouldHandle(any(), any())).thenReturn(true);

        Response success = new Response();
        success.setSuccess(true);
        when(queueChannel.dispatch(any())).thenReturn(Mono.just(success));

        FlexlbRequest request = createRequest(2L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(queueChannel).dispatch(request);
        verify(batchChannel, never()).dispatch(any());
        verify(directChannel, never()).dispatch(any());
    }

    @Test
    void route_usesBatchChannelWhenBatchAccepts() {
        when(batchChannel.shouldHandle(any(), any())).thenReturn(true);

        Response success = new Response();
        success.setSuccess(true);
        when(batchChannel.dispatch(any())).thenReturn(Mono.just(success));

        FlexlbRequest request = createRequest(3L, ScheduleModeEnum.BATCH);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(batchChannel).dispatch(request);
        verify(queueChannel, never()).dispatch(any());
        verify(directChannel, never()).dispatch(any());
    }

    @Test
    void route_doesNotReportOnFailure() {
        when(batchChannel.shouldHandle(any(), any())).thenReturn(false);
        when(queueChannel.shouldHandle(any(), any())).thenReturn(false);
        when(directChannel.shouldHandle(any(), any())).thenReturn(true);

        Response failure = new Response();
        failure.setSuccess(false);
        when(directChannel.dispatch(any())).thenReturn(Mono.just(failure));

        FlexlbRequest request = createRequest(4L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertFalse(result.isSuccess());
        verify(recentCacheKeyTraceReporter, never()).report(any());
    }

    @Test
    void route_setsConfigOnRequest() {
        when(batchChannel.shouldHandle(any(), any())).thenReturn(false);
        when(queueChannel.shouldHandle(any(), any())).thenReturn(false);
        when(directChannel.shouldHandle(any(), any())).thenReturn(true);

        Response success = new Response();
        success.setSuccess(true);
        when(directChannel.dispatch(any())).thenReturn(Mono.just(success));

        FlexlbRequest request = createRequest(5L, ScheduleModeEnum.AUTO);
        assertNull(request.getConfig());

        routeService.route(request).block();

        assertEquals(config, request.getConfig());
    }

    @Test
    void route_fallsThroughWhenBatchChannelAbsent() {
        // Only queue and direct channels — simulates BatchScheduler bean not existing
        routeService = new RouteService(configService,
                List.of(queueChannel, directChannel),
                recentCacheKeyTraceReporter);

        when(queueChannel.shouldHandle(any(), any())).thenReturn(false);
        when(directChannel.shouldHandle(any(), any())).thenReturn(true);

        Response success = new Response();
        success.setSuccess(true);
        when(directChannel.dispatch(any())).thenReturn(Mono.just(success));

        // Even with BATCH mode, falls through to direct since batch channel is absent
        FlexlbRequest request = createRequest(6L, ScheduleModeEnum.BATCH);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(directChannel).dispatch(request);
    }

    // ---- BatchScheduler.shouldHandle() ----

    @Test
    void shouldHandle_returnsTrueWhenModeIsBatch() {
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(10L, ScheduleModeEnum.BATCH);
        assertTrue(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_returnsFalseWhenModeIsDirect() {
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(11L, ScheduleModeEnum.DIRECT);
        assertFalse(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_autoReturnsFalseWhenBatchNotEnabled() {
        config.setFlexlbBatchEnabled(false);
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(12L, ScheduleModeEnum.AUTO);
        assertFalse(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_autoReturnsTrueWhenBatchEnabledAndCriteriaMatch() {
        config.setFlexlbBatchEnabled(true);
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(13L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);
        request.getRequest().setForceDisableSpRun(false);

        assertTrue(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_autoReturnsFalseWhenMaxNewTokensIsOne() {
        config.setFlexlbBatchEnabled(true);
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(14L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(1);
        request.getRequest().setNumBeams(1);

        assertFalse(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_autoReturnsFalseWhenGenerateInputPbBytesIsNull() {
        config.setFlexlbBatchEnabled(true);
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(15L, ScheduleModeEnum.AUTO);
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);

        assertFalse(scheduler.shouldHandle(request, config));
    }

    @Test
    void shouldHandle_autoReturnsFalseWhenForceDisableSpRun() {
        config.setFlexlbBatchEnabled(true);
        BatchScheduler scheduler = new BatchScheduler(null, null, null, null, null);
        FlexlbRequest request = createRequest(16L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);
        request.getRequest().setForceDisableSpRun(true);

        assertFalse(scheduler.shouldHandle(request, config));
    }

    // ---- helpers ----

    private FlexlbRequest createRequest(long requestId, ScheduleModeEnum mode) {
        Request request = new Request();
        request.setRequestId(requestId);
        request.setMaxNewTokens(1);
        request.setNumBeams(1);
        FlexlbRequest flexlbRequest = new FlexlbRequest(request);
        flexlbRequest.setScheduleMode(mode);
        return flexlbRequest;
    }
}
