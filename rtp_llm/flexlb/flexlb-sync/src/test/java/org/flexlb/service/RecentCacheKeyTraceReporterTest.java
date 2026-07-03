package org.flexlb.service;

import org.flexlb.cache.core.RecentCacheKeyWindow;
import org.flexlb.cache.monitor.CacheMetricsReporter;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RecentCacheKeyTraceReporterTest {

    @Mock
    private RecentCacheKeyWindow recentCacheKeyWindow;

    @Mock
    private CacheMetricsReporter cacheMetricsReporter;

    @InjectMocks
    private RecentCacheKeyTraceReporter reporter;

    private FlexlbConfig config;

    @BeforeEach
    void setUp() {
        config = new FlexlbConfig();
        // Disable theory log to avoid file I/O in tests
        config.setCacheHitTheoryLogEnabled(false);
    }

    @Test
    void report_nullRequest_doesNothing() {
        reporter.report(null);
        verify(recentCacheKeyWindow, never()).record(any());
    }

    @Test
    void report_configDisablesCacheHitWindow_doesNothing() {
        config.setCacheHitWindowWriteEnabled(false);

        FlexlbRequest request = createRequest(1L, List.of(10L, 20L), 100L);
        reporter.report(request);

        verify(recentCacheKeyWindow, never()).record(any());
    }

    @Test
    void report_nullMasterRequest_doesNothing() {
        FlexlbRequest request = new FlexlbRequest(null);
        request.setConfig(config);
        reporter.report(request);

        verify(recentCacheKeyWindow, never()).record(any());
    }

    @Test
    void report_nullConfig_proceedsWithDefaults() {
        // config is null on the request — report() should still proceed
        // since the null check is: config != null && !config.isCacheHitWindowWriteEnabled()
        // With null config, it skips the check and proceeds
        Request mockRequest = mock(Request.class);
        when(mockRequest.getBlockCacheKeys()).thenReturn(List.of(10L));
        when(mockRequest.getSeqLen()).thenReturn(100L);
        when(mockRequest.getCacheKeyBlockSize()).thenReturn(10L);

        FlexlbRequest request = new FlexlbRequest(mockRequest);
        // config is null (not set)

        RecentCacheKeyWindow.Snapshot snapshot = mock(RecentCacheKeyWindow.Snapshot.class);
        when(snapshot.getRequestHitOccurrences()).thenReturn(1L);
        when(snapshot.getRequestOccurrences()).thenReturn(1L);
        when(recentCacheKeyWindow.record(any())).thenReturn(snapshot);

        reporter.report(request);

        verify(recentCacheKeyWindow).record(List.of(10L));
    }

    @Test
    void report_recordsToWindowAndReportsMetrics() {
        config.setCacheHitWindowWriteEnabled(true);
        config.setCacheHitMetricReportEnabled(true);

        List<Long> cacheKeys = List.of(10L, 20L, 30L);
        FlexlbRequest request = createRequest(1L, cacheKeys, 100L);

        RecentCacheKeyWindow.Snapshot snapshot = mock(RecentCacheKeyWindow.Snapshot.class);
        when(snapshot.getRequestHitOccurrences()).thenReturn(2L);
        when(snapshot.getRequestOccurrences()).thenReturn(3L);
        when(snapshot.getTimeWindowMs()).thenReturn(30L * 60 * 1000);
        when(recentCacheKeyWindow.record(cacheKeys)).thenReturn(snapshot);

        reporter.report(request);

        verify(recentCacheKeyWindow).record(cacheKeys);
        verify(cacheMetricsReporter).reportRecentCacheKeyHitMetrics(
                anyLong(), anyLong(), anyLong());
    }

    @Test
    void report_configDisablesMetricReport_doesNotReportMetrics() {
        config.setCacheHitWindowWriteEnabled(true);
        config.setCacheHitMetricReportEnabled(false);

        List<Long> cacheKeys = List.of(10L, 20L);
        FlexlbRequest request = createRequest(2L, cacheKeys, 50L);

        RecentCacheKeyWindow.Snapshot snapshot = mock(RecentCacheKeyWindow.Snapshot.class);
        when(snapshot.getRequestHitOccurrences()).thenReturn(1L);
        when(snapshot.getRequestOccurrences()).thenReturn(2L);
        when(snapshot.getTimeWindowMs()).thenReturn(30L * 60 * 1000);
        when(recentCacheKeyWindow.record(cacheKeys)).thenReturn(snapshot);

        reporter.report(request);

        verify(recentCacheKeyWindow).record(cacheKeys);
        verify(cacheMetricsReporter, never()).reportRecentCacheKeyHitMetrics(
                anyLong(), anyLong(), anyLong());
    }

    // ---- helpers ----

    private FlexlbRequest createRequest(long requestId, List<Long> cacheKeys, long seqLen) {
        Request mockRequest = mock(Request.class);
        when(mockRequest.getRequestId()).thenReturn(requestId);
        when(mockRequest.getSeqLen()).thenReturn(seqLen);
        when(mockRequest.getBlockCacheKeys()).thenReturn(cacheKeys);
        when(mockRequest.getCacheKeyBlockSize()).thenReturn(10L);
        when(mockRequest.getRequestTimeMs()).thenReturn(System.currentTimeMillis());

        FlexlbRequest request = new FlexlbRequest(mockRequest);
        request.setConfig(config);
        return request;
    }
}
