package org.flexlb.service;

import org.flexlb.balance.scheduler.BatchScheduler;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.QueueManager;
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

import java.util.concurrent.CompletableFuture;

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
    private DefaultRouter router;
    @Mock
    private QueueManager queueManager;
    @Mock
    private BatchScheduler batchScheduler;
    @Mock
    private RecentCacheKeyTraceReporter recentCacheKeyTraceReporter;

    private RouteService routeService;
    private FlexlbConfig config;

    @BeforeEach
    void setUp() {
        config = new FlexlbConfig();
        lenient().when(configService.loadBalanceConfig()).thenReturn(config);
        routeService = new RouteService(configService, router, queueManager, batchScheduler, recentCacheKeyTraceReporter);
    }

    // ---- route() path selection ----

    @Test
    void route_usesDirectRoutingWhenBatchDisabledAndQueueDisabled() {
        config.setFlexlbBatchEnabled(false);
        config.setEnableQueueing(false);

        Response success = new Response();
        success.setSuccess(true);
        when(router.route(any(FlexlbRequest.class))).thenReturn(success);

        FlexlbRequest request = createRequest(1L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(router).route(request);
        verify(queueManager, never()).tryRouteAsync(any());
        verify(batchScheduler, never()).submit(any());
        verify(recentCacheKeyTraceReporter).report(request);
    }

    @Test
    void route_usesQueueRoutingWhenBatchDisabledAndQueueEnabled() {
        config.setFlexlbBatchEnabled(false);
        config.setEnableQueueing(true);

        Response success = new Response();
        success.setSuccess(true);
        when(queueManager.tryRouteAsync(any(FlexlbRequest.class))).thenReturn(Mono.just(success));

        FlexlbRequest request = createRequest(2L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(queueManager).tryRouteAsync(request);
        verify(router, never()).route(any());
        verify(batchScheduler, never()).submit(any());
    }

    @Test
    void route_usesBatchRoutingWhenModeIsBatch() {
        Response success = new Response();
        success.setSuccess(true);
        when(batchScheduler.submit(any(FlexlbRequest.class)))
                .thenReturn(CompletableFuture.completedFuture(success));

        FlexlbRequest request = createRequest(3L, ScheduleModeEnum.BATCH);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertTrue(result.isSuccess());
        verify(batchScheduler).submit(request);
        verify(router, never()).route(any());
        verify(queueManager, never()).tryRouteAsync(any());
    }

    @Test
    void route_doesNotReportOnFailure() {
        config.setFlexlbBatchEnabled(false);
        config.setEnableQueueing(false);

        Response failure = new Response();
        failure.setSuccess(false);
        when(router.route(any(FlexlbRequest.class))).thenReturn(failure);

        FlexlbRequest request = createRequest(4L, ScheduleModeEnum.AUTO);
        Response result = routeService.route(request).block();

        assertNotNull(result);
        assertFalse(result.isSuccess());
        verify(recentCacheKeyTraceReporter, never()).report(any());
    }

    @Test
    void route_setsConfigOnRequest() {
        config.setFlexlbBatchEnabled(false);
        config.setEnableQueueing(false);

        Response success = new Response();
        success.setSuccess(true);
        when(router.route(any(FlexlbRequest.class))).thenReturn(success);

        FlexlbRequest request = createRequest(5L, ScheduleModeEnum.AUTO);
        assertNull(request.getConfig());

        routeService.route(request).block();

        assertEquals(config, request.getConfig());
    }

    // ---- shouldUseFlexlbBatch() ----

    @Test
    void shouldUseFlexlbBatch_returnsTrueWhenModeIsBatch() {
        FlexlbRequest request = createRequest(10L, ScheduleModeEnum.BATCH);
        assertTrue(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_returnsFalseWhenModeIsDirect() {
        FlexlbRequest request = createRequest(11L, ScheduleModeEnum.DIRECT);
        assertFalse(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_autoReturnsFalseWhenBatchNotEnabled() {
        config.setFlexlbBatchEnabled(false);
        FlexlbRequest request = createRequest(12L, ScheduleModeEnum.AUTO);
        assertFalse(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_autoReturnsTrueWhenBatchEnabledAndCriteriaMatch() {
        config.setFlexlbBatchEnabled(true);
        FlexlbRequest request = createRequest(13L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);
        request.getRequest().setForceDisableSpRun(false);

        assertTrue(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_autoReturnsFalseWhenMaxNewTokensIsOne() {
        config.setFlexlbBatchEnabled(true);
        FlexlbRequest request = createRequest(14L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(1);
        request.getRequest().setNumBeams(1);

        assertFalse(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_autoReturnsFalseWhenGenerateInputPbBytesIsNull() {
        config.setFlexlbBatchEnabled(true);
        FlexlbRequest request = createRequest(15L, ScheduleModeEnum.AUTO);
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);

        assertFalse(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_autoReturnsFalseWhenForceDisableSpRun() {
        config.setFlexlbBatchEnabled(true);
        FlexlbRequest request = createRequest(16L, ScheduleModeEnum.AUTO);
        request.setGenerateInputPbBytes(new byte[]{1, 2, 3});
        request.getRequest().setMaxNewTokens(100);
        request.getRequest().setNumBeams(1);
        request.getRequest().setForceDisableSpRun(true);

        assertFalse(routeService.shouldUseFlexlbBatch(request, config));
    }

    @Test
    void shouldUseFlexlbBatch_returnsFalseWhenBatchSchedulerIsNull() {
        RouteService serviceWithoutBatch = new RouteService(
                configService, router, queueManager, null, recentCacheKeyTraceReporter);

        FlexlbRequest request = createRequest(17L, ScheduleModeEnum.BATCH);
        assertFalse(serviceWithoutBatch.shouldUseFlexlbBatch(request, config));
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
