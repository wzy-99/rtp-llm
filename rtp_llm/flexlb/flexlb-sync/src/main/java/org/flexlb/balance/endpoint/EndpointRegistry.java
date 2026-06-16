package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.FlexlbBatchScheduler;
import org.flexlb.config.ConfigService;
import org.flexlb.dao.master.WorkerStatus;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.util.concurrent.ConcurrentHashMap;

@Component
public class EndpointRegistry {

    private final ConcurrentHashMap<String, PrefillEndpoint> prefillEndpoints = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, DecodeEndpoint> decodeEndpoints = new ConcurrentHashMap<>();
    private final ConfigService configService;
    private final FlexlbBatchScheduler batchScheduler;

    public EndpointRegistry(ConfigService configService,
                            @Lazy FlexlbBatchScheduler batchScheduler) {
        this.configService = configService;
        this.batchScheduler = batchScheduler;
    }

    public WorkerEndpoint get(String ipPort) {
        WorkerEndpoint ep = prefillEndpoints.get(ipPort);
        if (ep != null) {
            return ep;
        }
        return decodeEndpoints.get(ipPort);
    }

    public PrefillEndpoint getPrefill(String ipPort) {
        return prefillEndpoints.get(ipPort);
    }

    public DecodeEndpoint getDecode(String ipPort) {
        return decodeEndpoints.get(ipPort);
    }

    public PrefillEndpoint ensurePrefillEndpoint(String ipPort, WorkerStatus status) {
        return prefillEndpoints.computeIfAbsent(ipPort,
                k -> new PrefillEndpoint(status, configService.loadBalanceConfig(), batchScheduler));
    }

    public DecodeEndpoint ensureDecodeEndpoint(String ipPort, WorkerStatus status) {
        return decodeEndpoints.computeIfAbsent(ipPort,
                k -> new DecodeEndpoint(status));
    }

    public void close() {
        prefillEndpoints.values().forEach(WorkerEndpoint::close);
        decodeEndpoints.values().forEach(WorkerEndpoint::close);
    }

    /**
     * Trigger TTL eviction on all prefill and decode endpoints.
     * Called periodically by the scheduler.
     *
     * @param ttlMs max age before eviction
     */
    public void evictExpiredAll(long ttlMs) {
        prefillEndpoints.values().forEach(ep -> ep.evictExpiredBatches(ttlMs));
        decodeEndpoints.values().forEach(ep -> ep.evictExpiredRequests(ttlMs));
    }
}
