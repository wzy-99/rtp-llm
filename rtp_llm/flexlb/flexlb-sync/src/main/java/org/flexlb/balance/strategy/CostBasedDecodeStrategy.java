package org.flexlb.balance.strategy;

import org.apache.commons.collections4.CollectionUtils;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.resource.DecodeResourceMeasure;
import org.flexlb.balance.resource.ResourceMeasureFactory;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.EndpointFeasibility;
import org.flexlb.dao.loadbalance.RejectionReason;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.LoadBalanceStrategyEnum;
import org.flexlb.enums.ResourceMeasureIndicatorEnum;
import org.flexlb.sync.status.EngineWorkerStatus;
import org.flexlb.util.CommonUtils;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

@Component("costBasedDecodeStrategy")
public class CostBasedDecodeStrategy implements LoadBalanceStrategy {

    private final EngineWorkerStatus engineWorkerStatus;
    private final double decayFactor;
    private final ResourceMeasureFactory resourceMeasureFactory;

    public CostBasedDecodeStrategy(ConfigService configService,
                                    EngineWorkerStatus engineWorkerStatus,
                                    ResourceMeasureFactory resourceMeasureFactory) {
        this.engineWorkerStatus = engineWorkerStatus;
        FlexlbConfig config = configService.loadBalanceConfig();
        this.decayFactor = config.getWeightedCacheDecayFactor();
        this.resourceMeasureFactory = resourceMeasureFactory;
        LoadBalanceStrategyFactory.register(LoadBalanceStrategyEnum.COST_BASED_DECODE, this);
    }

    @Override
    public ServerStatus select(BalanceContext balanceContext, RoleType roleType, String group) {
        Request request = balanceContext.getRequest();
        long seqLen = request.getSeqLen();
        long maxNewTokens = request.getMaxNewTokens();
        long expectedKvTokens = seqLen + maxNewTokens;
        FlexlbConfig config = balanceContext.getConfig();

        EndpointFilterResult filterResult = getAvailableEndpoints(roleType, group, config.getResourceMeasureIndicator(roleType), config, seqLen);
        List<DecodeEndpoint> eligible = filterResult.survivors();
        if (CollectionUtils.isEmpty(eligible)) {
            Logger.warn("Decode select failed: no available endpoints, request_id={}, rejected={}",
                    balanceContext.getRequestId(), filterResult.rejected());
            return ServerStatus.failureWithFeasibility(StrategyErrorType.NO_AVAILABLE_WORKER, filterResult.rejected());
        }

        FilterResult hardFilterResult = applyHardFilters(eligible, seqLen, config);
        List<DecodeEndpoint> survivors = hardFilterResult.survivors();

        DecodeEndpoint selectedEndpoint = weightedRandomSelection(survivors);

        if (selectedEndpoint != null) {
            return buildServerStatus(selectedEndpoint, seqLen, expectedKvTokens,
                    roleType, balanceContext.getRequestId(), balanceContext.getPriority());
        }

        List<EndpointFeasibility> allRejected = new ArrayList<>();
        allRejected.addAll(filterResult.rejected());
        allRejected.addAll(hardFilterResult.rejected());
        Logger.warn("Decode select failed: all filtered out, request_id={}, rejected={}",
                balanceContext.getRequestId(), allRejected);
        return ServerStatus.failureWithFeasibility(StrategyErrorType.NO_AVAILABLE_WORKER, allRejected);
    }

    private record EndpointFilterResult(List<DecodeEndpoint> survivors, List<EndpointFeasibility> rejected) {}
    private record FilterResult(List<DecodeEndpoint> survivors, List<EndpointFeasibility> rejected) {}

    private EndpointFilterResult getAvailableEndpoints(RoleType roleType, String group, ResourceMeasureIndicatorEnum indicator, FlexlbConfig config, long seqLen) {
        DecodeResourceMeasure measure = (DecodeResourceMeasure) resourceMeasureFactory.getMeasure(indicator);
        if (measure == null) {
            return new EndpointFilterResult(new ArrayList<>(), List.of(new EndpointFeasibility("", RejectionReason.NO_REGISTERED, 0, 0, 0)));
        }
        List<DecodeEndpoint> result = new ArrayList<>(engineWorkerStatus.getModelWorkerCapacity(roleType));
        List<EndpointFeasibility> rejected = new ArrayList<>();
        int registered = engineWorkerStatus.forEachModelWorkerEndpoint(roleType, group, (ipPort, ep) -> {
            if (!(ep instanceof DecodeEndpoint de)) {
                return;
            }
            if (!de.getStatus().isAlive()) {
                rejected.add(new EndpointFeasibility(de.ipPort(), RejectionReason.NOT_ALIVE, 0, 0, 0));
                return;
            }
            if (!measure.isResourceAvailable(de)) {
                long concurrencyLimit = config.getDecodeConcurrencyLimit();
                long slotDeficit = (concurrencyLimit > 0 && de.getTotalLoad() >= concurrencyLimit)
                        ? Math.max(0, de.getTotalLoad() + 1 - concurrencyLimit) : 0;
                long availableKv = de.realKvAvailable();
                long kvDeficit = Math.max(0, seqLen - availableKv);

                if (slotDeficit > 0 && kvDeficit > 0) {
                    // Both slot and KV saturated — populate both deficits so the
                    // EvictionPlanner dispatches to Case 4 (combined eviction).
                    rejected.add(new EndpointFeasibility(de.ipPort(),
                            RejectionReason.KV_UNAVAILABLE, kvDeficit, slotDeficit, 0));
                } else if (slotDeficit > 0) {
                    rejected.add(new EndpointFeasibility(de.ipPort(),
                            RejectionReason.COMPUTE_SATURATED, 0, slotDeficit, 0));
                } else {
                    rejected.add(new EndpointFeasibility(de.ipPort(),
                            RejectionReason.KV_UNAVAILABLE, kvDeficit, 0, 0));
                }
                return;
            }
            result.add(de);
        });
        if (registered == 0) {
            return new EndpointFilterResult(result, List.of(new EndpointFeasibility("", RejectionReason.NO_REGISTERED, 0, 0, 0)));
        }
        return new EndpointFilterResult(result, rejected);
    }

    @Override
    public void rollBack(WorkerEndpoint ep, long requestId) {
        Logger.debug("Decode rollBack - ip: {}, requestId: {}", ep.ipPort(), requestId);

        if (ep instanceof DecodeEndpoint de) {
            de.release(requestId);
        }
    }

    private FilterResult applyHardFilters(List<DecodeEndpoint> eligible, long seqLen, FlexlbConfig config) {
        double hotspotMultiplier = config.getDecodeHotspotMultiplier();
        double imbalanceMultiplier = config.getDecodeImbalanceMultiplier();

        int n = eligible.size();
        // 缓存每个 endpoint 的值，避免重复调用
        long[] loads = new long[n];
        long[] kvUseds = new long[n];
        long sumLoad = 0;
        long sumCacheUsed = 0;
        for (int i = 0; i < n; i++) {
            DecodeEndpoint ep = eligible.get(i);
            loads[i] = ep.getTotalLoad();
            kvUseds[i] = ep.realKvUsed();
            sumLoad += loads[i];
            sumCacheUsed += kvUseds[i];
        }
        long avgLoad = sumLoad / n;
        long avgCacheUsed = sumCacheUsed / n;

        List<DecodeEndpoint> survivors = new ArrayList<>(n);
        List<EndpointFeasibility> rejected = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            DecodeEndpoint ep = eligible.get(i);
            long availableKv = ep.realKvAvailable();
            long totalKv = ep.realKvTotal();
            if (totalKv > 0 && availableKv < seqLen) {
                long kvDeficit = Math.max(0, seqLen - availableKv);
                rejected.add(new EndpointFeasibility(ep.ipPort(), RejectionReason.KV_CAPACITY, kvDeficit, 0, 0));
                continue;
            }
            if (hotspotMultiplier > 0 && avgLoad > 0
                    && loads[i] > avgLoad * hotspotMultiplier) {
                rejected.add(new EndpointFeasibility(ep.ipPort(), RejectionReason.HOTSPOT_FILTERED, 0, 0, 0));
                continue;
            }
            if (imbalanceMultiplier > 0 && avgCacheUsed > 0
                    && kvUseds[i] > avgCacheUsed * imbalanceMultiplier) {
                rejected.add(new EndpointFeasibility(ep.ipPort(), RejectionReason.IMBALANCE_FILTERED, 0, 0, 0));
                continue;
            }
            survivors.add(ep);
        }

        return new FilterResult(survivors, rejected);
    }

    private DecodeEndpoint weightedRandomSelection(List<DecodeEndpoint> candidateEndpoints) {
        if (candidateEndpoints.isEmpty()) {
            return null;
        }

        int n = candidateEndpoints.size();
        // 缓存 realKvUsed() 避免重复调用
        long[] cacheUsed = new long[n];
        int minCacheUsedIdx = 0;
        int maxCacheUsedIdx = 0;
        for (int i = 0; i < n; i++) {
            cacheUsed[i] = candidateEndpoints.get(i).realKvUsed();
            if (cacheUsed[i] < cacheUsed[minCacheUsedIdx]) {
                minCacheUsedIdx = i;
            }
            if (cacheUsed[i] > cacheUsed[maxCacheUsedIdx]) {
                maxCacheUsedIdx = i;
            }
        }

        double[] weights = new double[n];
        double totalWeight = 0;
        boolean allSameUsage = true;
        long firstCacheUsed = cacheUsed[0];
        // Subtract the value that produces the maximum log-weight before exponentiation.
        // This is mathematically equivalent to the previous average-centered weights, but
        // keeps every exponent <= 0 and avoids exp(...) overflowing for large KV gaps.
        long referenceCacheUsed = decayFactor >= 0
                ? cacheUsed[minCacheUsedIdx]
                : cacheUsed[maxCacheUsedIdx];
        for (int i = 0; i < n; i++) {
            if (cacheUsed[i] != firstCacheUsed) {
                allSameUsage = false;
            }
            double normalizedValue = (double) cacheUsed[i] - referenceCacheUsed;
            weights[i] = Math.exp(-decayFactor * normalizedValue);
            totalWeight += weights[i];
        }

        if (allSameUsage) {
            // 所有 endpoint 使用率相同，随机选一个
            return candidateEndpoints.get(ThreadLocalRandom.current().nextInt(n));
        }
        if (!Double.isFinite(totalWeight) || totalWeight <= 0) {
            Logger.warn("Decode weighted selection produced invalid total weight: decayFactor={}, totalWeight={}",
                    decayFactor, totalWeight);
            return candidateEndpoints.get(minCacheUsedIdx);
        }

        // 加权随机选择
        double r = ThreadLocalRandom.current().nextDouble(totalWeight);
        double cumulativeWeight = 0;
        for (int i = 0; i < n; i++) {
            cumulativeWeight += weights[i];
            if (r <= cumulativeWeight) {
                return candidateEndpoints.get(i);
            }
        }

        // fallback: 返回使用率最低的
        return candidateEndpoints.get(minCacheUsedIdx);
    }

    private ServerStatus buildServerStatus(DecodeEndpoint optimalEndpoint, long seqLen,
                                           long expectedKvTokens, RoleType roleType,
                                           long requestId, int priority) {
        ServerStatus result = new ServerStatus();
        try {
            // All schedule modes (BATCH, DIRECT, QUEUE) reserve decode KV to prevent
            // over-admission before the engine reports the new load.
            //
            // Cap expectedKvTokens to the endpoint's total KV capacity. When
            // maxNewTokens is very large (e.g. 8192), the raw sum seqLen +
            // maxNewTokens may exceed the physical KV limit, causing
            // inflightKvReserved() to be artificially inflated and scoring
            // to become overly conservative.
            long totalKv = optimalEndpoint.realKvTotal();
            if (totalKv > 0 && expectedKvTokens > totalKv) {
                expectedKvTokens = totalKv;
            }
            optimalEndpoint.reserve(requestId, seqLen, expectedKvTokens, priority);

            result.setSuccess(true);
            result.setRole(roleType);
            result.setServerIp(optimalEndpoint.getIp());
            result.setHttpPort(optimalEndpoint.getHttpPort());
            result.setGrpcPort(CommonUtils.toGrpcPort(optimalEndpoint.getHttpPort()));
            result.setDpRank(optimalEndpoint.getStatus().getDpRank());
            result.setGroup(optimalEndpoint.getStatus().getGroup());
            result.setRequestId(requestId);
        } catch (Exception e) {
            Logger.error("buildServerStatus error", e);
            result.setSuccess(false);
            result.setCode(StrategyErrorType.NO_AVAILABLE_WORKER.getErrorCode());
            result.setMessage(StrategyErrorType.NO_AVAILABLE_WORKER.getErrorMsg());
        }
        return result;
    }
}
