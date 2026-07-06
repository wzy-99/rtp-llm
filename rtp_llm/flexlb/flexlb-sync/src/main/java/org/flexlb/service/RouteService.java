package org.flexlb.service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.flexlb.balance.scheduler.AbstractScheduler;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class RouteService {

    private final ConfigService configService;
    private final List<AbstractScheduler> schedulers;
    private final RecentCacheKeyTraceReporter recentCacheKeyTraceReporter;

    public RouteService(ConfigService configService,
                        List<AbstractScheduler> schedulers,
                        RecentCacheKeyTraceReporter recentCacheKeyTraceReporter) {
        this.configService = configService;
        // Sort schedulers in descending priority order (highest order evaluated first)
        this.schedulers = schedulers.stream()
                .sorted(Comparator.comparingInt(AbstractScheduler::getOrder).reversed())
                .collect(Collectors.toList());
        this.recentCacheKeyTraceReporter = recentCacheKeyTraceReporter;
    }

    /**
     * Route request to the highest-priority scheduler that accepts it.
     *
     * <p>Iterates schedulers in descending priority order. The first scheduler
     * whose {@code shouldHandle()} returns {@code true} handles the request.
     *
     * @param request FlexLB request
     * @return Routing result
     */
    public Mono<Response> route(FlexlbRequest request) {
        FlexlbConfig flexlbConfig = configService.loadBalanceConfig();
        request.setConfig(flexlbConfig);

        for (AbstractScheduler scheduler : schedulers) {
            if (scheduler.shouldHandle(request, flexlbConfig)) {
                return scheduler.dispatch(request).doOnSuccess(result -> {
                    request.setResponse(result);
                    if (result != null && result.isSuccess()) {
                        recentCacheKeyTraceReporter.report(request);
                    }
                });
            }
        }

        // No scheduler accepted the request — should not happen since DirectScheduler
        // always returns true for shouldHandle()
        return Mono.just(Response.error(StrategyErrorType.NO_AVAILABLE_WORKER));
    }
}
