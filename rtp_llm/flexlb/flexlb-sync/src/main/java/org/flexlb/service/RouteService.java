package org.flexlb.service;

import java.util.concurrent.CompletableFuture;

import org.flexlb.balance.scheduler.BatchScheduler;
import org.flexlb.balance.scheduler.DefaultRouter;
import org.flexlb.balance.scheduler.QueueManager;
import org.flexlb.balance.scheduler.Router;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.enums.ScheduleModeEnum;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Component
public class RouteService {

    private final ConfigService configService;
    private final Router router;
    private final QueueManager queueManager;
    private final BatchScheduler batchScheduler;
    private final RecentCacheKeyTraceReporter recentCacheKeyTraceReporter;

    public RouteService(ConfigService configService,
                        DefaultRouter defaultScheduler,
                        QueueManager queueManager,
                        @Lazy @Autowired(required = false) BatchScheduler batchScheduler,
                        RecentCacheKeyTraceReporter recentCacheKeyTraceReporter) {
        this.configService = configService;
        this.router = defaultScheduler;
        this.queueManager = queueManager;
        this.batchScheduler = batchScheduler;
        this.recentCacheKeyTraceReporter = recentCacheKeyTraceReporter;
    }

    /**
     * Route request to appropriate workers
     * @param request FlexLB request
     * @return Routing result
     */
    public Mono<Response> route(FlexlbRequest request) {
        FlexlbConfig flexlbConfig = configService.loadBalanceConfig();
        request.setConfig(flexlbConfig);

        Mono<Response> resultMono;
        if (shouldUseFlexlbBatch(request, flexlbConfig)) {
            CompletableFuture<Response> future = batchScheduler.submit(request);
            resultMono = Mono.fromFuture(future);
        } else if (flexlbConfig.isEnableQueueing()) {
            resultMono = queueManager.tryRouteAsync(request);  // Use async queuing mechanism
        } else {
            resultMono = Mono.fromCallable(() -> router.route(request));  // Direct routing without queuing
        }

        return resultMono.doOnSuccess(result -> {
            request.setResponse(result);
            if (result != null && result.isSuccess()) {
                recentCacheKeyTraceReporter.report(request);
            }
        });
    }

    boolean shouldUseFlexlbBatch(FlexlbRequest request, FlexlbConfig config) {
        if (batchScheduler == null || config == null) {
            return false;
        }
        ScheduleModeEnum mode = request.getScheduleMode();
        if (mode == ScheduleModeEnum.BATCH) {
            return true;
        }
        if (mode == ScheduleModeEnum.DIRECT) {
            return false;
        }
        // AUTO: use batch when config enables it and request characteristics match
        if (!config.isFlexlbBatchEnabled()) {
            return false;
        }
        Request masterRequest = request.getRequest();
        return masterRequest != null
                && masterRequest.getMaxNewTokens() > 1
                && masterRequest.getNumBeams() <= 1
                && !masterRequest.isForceDisableSpRun()
                && request.getGenerateInputPbBytes() != null
                && request.getGenerateInputPbBytes().length > 0;
    }
}
