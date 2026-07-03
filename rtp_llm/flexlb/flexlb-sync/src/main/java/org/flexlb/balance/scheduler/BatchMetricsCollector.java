package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Map;

/**
 * Periodic batch metrics reporter — extracts metrics collection from the
 * former {@code FlexlbBatchScheduler} God Class.
 *
 * <p>Reports:
 * <ul>
 *   <li>Scheduler-level inflight size</li>
 *   <li>Per-prefill-endpoint batch metrics</li>
 *   <li>Per-decode-endpoint batch metrics</li>
 * </ul>
 */
@Component
public class BatchMetricsCollector {

    private final BatchInflightStore store;
    private final EndpointRegistry endpointRegistry;
    private final BatchSchedulerReporter reporter;

    public BatchMetricsCollector(BatchInflightStore store,
                                 EndpointRegistry endpointRegistry,
                                 BatchSchedulerReporter reporter) {
        this.store = store;
        this.endpointRegistry = endpointRegistry;
        this.reporter = reporter;
    }

    @Scheduled(fixedRate = 20000L)
    public void reportBatchMetrics() {
        reporter.reportSchedulerInflightSize(store.size());

        // Per-worker metrics: prefill endpoints
        for (Map.Entry<String, PrefillEndpoint> entry : endpointRegistry.getPrefillEndpoints().entrySet()) {
            entry.getValue().reportBatchMetrics(reporter);
        }

        // Per-worker metrics: decode endpoints
        for (Map.Entry<String, DecodeEndpoint> entry : endpointRegistry.getDecodeEndpoints().entrySet()) {
            entry.getValue().reportBatchMetrics(reporter);
        }
    }
}
