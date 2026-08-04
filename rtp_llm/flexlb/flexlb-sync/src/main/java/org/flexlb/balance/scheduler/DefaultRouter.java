package org.flexlb.balance.scheduler;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.EndpointRegistry;
import org.flexlb.balance.endpoint.EvictionVictim;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.balance.endpoint.WorkerEndpoint;
import org.flexlb.balance.planner.EvictionPlanner;
import org.flexlb.balance.policy.GroupRoutingDecision;
import org.flexlb.balance.policy.GroupRoutingPolicy;
import org.flexlb.balance.strategy.LoadBalanceStrategy;
import org.flexlb.balance.strategy.LoadBalanceStrategyFactory;
import org.flexlb.service.monitor.BatchSchedulerReporter;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.DebugInfo;
import org.flexlb.dao.loadbalance.EndpointFeasibility;
import org.flexlb.dao.loadbalance.EvictionPlan;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.RoutingResult;
import org.flexlb.dao.loadbalance.RejectionReason;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.dao.loadbalance.StrategyErrorType;
import org.flexlb.dao.route.RoleType;
import org.flexlb.enums.LoadBalanceStrategyEnum;
import org.flexlb.enums.ScheduleModeEnum;
import org.flexlb.schedule.grpc.FlexlbScheduleProtocol.CancelReasonPB;
import org.flexlb.sync.status.EngineWorkerStatus;
import org.flexlb.sync.status.ModelWorkerStatus;
import org.flexlb.util.CommonUtils;
import org.flexlb.util.Logger;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import static org.flexlb.dao.loadbalance.StrategyErrorType.NO_AVAILABLE_WORKER;

@Component
@DependsOn({"randomStrategy", "costBasedDecodeStrategy", "costBasedPrefillStrategy", "shortestTtftStrategy"})
public class DefaultRouter implements Router {

    private final Map<RoleType, LoadBalanceStrategy> loadBalanceStrategyMap;
    private final GroupRoutingPolicy groupRoutingPolicy;
    private final EndpointRegistry endpointRegistry;
    private final EngineWorkerStatus engineWorkerStatus;
    private final ObjectProvider<CancelHandler> cancelHandlerProvider;
    private final ObjectProvider<BatchSchedulerReporter> reporterProvider;
    private final EvictionPlanner evictionPlanner;

    public DefaultRouter(ConfigService configService, GroupRoutingPolicy groupRoutingPolicy,
                         EndpointRegistry endpointRegistry,
                         EngineWorkerStatus engineWorkerStatus,
                         ObjectProvider<CancelHandler> cancelHandlerProvider,
                         ObjectProvider<BatchSchedulerReporter> reporterProvider,
                         EvictionPlanner evictionPlanner) {
        this.groupRoutingPolicy = groupRoutingPolicy;
        this.endpointRegistry = endpointRegistry;
        this.engineWorkerStatus = engineWorkerStatus;
        this.cancelHandlerProvider = cancelHandlerProvider;
        this.reporterProvider = reporterProvider;
        this.evictionPlanner = evictionPlanner;
        FlexlbConfig config = configService.loadBalanceConfig();
        this.loadBalanceStrategyMap = new EnumMap<>(RoleType.class);

        for (RoleType roleType : RoleType.values()) {
            LoadBalanceStrategyEnum strategy = config.getStrategyForRoleType(roleType);
            if (strategy != null) {
                loadBalanceStrategyMap.put(roleType, LoadBalanceStrategyFactory.getLoadBalanceStrategy(strategy));
            }
        }
    }

    @Override
    public Response route(BalanceContext balanceContext) {
        Response validationResponse = validateRequest(balanceContext);
        if (validationResponse != null) {
            return validationResponse;
        }

        ModelWorkerStatus workerStatus = EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS;
        List<RoleType> roleTypeList = workerStatus.getRoleTypeList();
        if (CollectionUtils.isEmpty(roleTypeList)) {
            Logger.warn("No worker roles registered yet (total workers: {})", workerStatus.getWorkerTotalCount());
            return Response.error(NO_AVAILABLE_WORKER);
        }

        RoutingResult routingResult = routeByRoleType(balanceContext, roleTypeList);

        if (routingResult.success()) {
            return buildSuccessResponse(routingResult.serverStatusList());
        }

        rollBackRoutingFailure(balanceContext, routingResult);
        return buildFailureResponse(routingResult);
    }

    private Response validateRequest(BalanceContext balanceContext) {
        if (balanceContext.getRequest() == null) {
            Logger.error("masterRequest is null");
            return Response.error(StrategyErrorType.INVALID_REQUEST);
        }

        if (EngineWorkerStatus.MODEL_ROLE_WORKER_STATUS == null) {
            Logger.error("targetModelRoleWorkerStatus is null");
            return Response.error(NO_AVAILABLE_WORKER);
        }

        return null;
    }

    /**
     * Execute routing decision, select optimal server for each role type.
     *
     * <p>Per-stage eviction: when select fails for a role, {@link #handleSelectFailure}
     * is called to attempt priority-based eviction + pendingHold direct assignment.
     * If eviction succeeds, routing continues to the next role; if not, the routing
     * fails for this role.
     */
    private RoutingResult routeByRoleType(BalanceContext balanceContext, List<RoleType> roleTypeList) {
        List<ServerStatus> serverStatusList = new ArrayList<>();
        GroupRoutingDecision groupRoutingDecision = groupRoutingPolicy.route(balanceContext);
        String policyGroup = groupRoutingDecision.group();
        String group = policyGroup;
        if (groupRoutingDecision.hasGroup()) {
            Logger.info("Group routing policy selected group, requestId: {}, policy: {}, group: {}",
                    balanceContext.getRequestId(), groupRoutingDecision.policyName(), group);
        }

        for (RoleType roleType : roleTypeList) {
            LoadBalanceStrategy loadBalanceStrategy = getLoadBalanceStrategy(roleType);
            ServerStatus serverStatus = loadBalanceStrategy.select(balanceContext, roleType, group);

            if (!serverStatus.isSuccess()) {
                // Attempt priority-based eviction + pendingHold direct assignment
                ServerStatus evictedStatus = handleSelectFailure(balanceContext, roleType, group, serverStatus);
                if (evictedStatus != null && evictedStatus.isSuccess()) {
                    Logger.info("Priority eviction succeeded for role {}, requestId={}, ep={}",
                            roleType.getCode(), balanceContext.getRequestId(),
                            evictedStatus.getServerIp() + ":" + evictedStatus.getHttpPort());
                    serverStatus = evictedStatus;
                } else {
                    Logger.warn("Failed to select {} worker: {}", roleType.getCode(), serverStatus.getMessage());
                    return RoutingResult.failure(serverStatusList, roleType, serverStatus.getMessage());
                }
            }

            serverStatusList.add(serverStatus);

            if (StringUtils.isBlank(policyGroup)) {
                group = serverStatus.getGroup();
            }
        }

        return RoutingResult.success(serverStatusList);
    }

    private LoadBalanceStrategy getLoadBalanceStrategy(RoleType roleType) {
        return loadBalanceStrategyMap.get(roleType);
    }

    // ==================== V4 Cost-Based Eviction Planner ====================

    /**
     * V4 cost-based eviction: use {@link EvictionPlanner} to plan, select, and
     * commit the minimum-cost eviction across all rejected endpoints.
     *
     * <p>Enabled by {@code flexlbV4EvictionEnabled}. Requires BATCH schedule mode.
     * If the v4 planner produces no feasible plan or commit fails, returns null
     * so the caller returns a failure status — v4 is the primary eviction approach
     * and does not fall through to the Phase 1 greedy eviction handlers.
     *
     * @return a success ServerStatus for the evicted endpoint, or null if v4 eviction failed
     */
    private ServerStatus handleSelectFailureV4(BalanceContext ctx, RoleType roleType,
                                                 String group, ServerStatus failure) {
        FlexlbConfig config = ctx.getConfig();
        if (config == null || !config.isFlexlbV4EvictionEnabled()) {
            return null;
        }
        if (ctx.getScheduleMode() != ScheduleModeEnum.BATCH) {
            return null;
        }

        List<EndpointFeasibility> feasibilities = failure.getEndpointFeasibilities();
        if (feasibilities == null || feasibilities.isEmpty()) {
            return null;
        }

        List<EvictionPlan> plans = evictionPlanner.planAll(feasibilities, ctx, roleType, group);
        EvictionPlan best = evictionPlanner.selectBest(plans);
        if (best == null) {
            Logger.warn("V4 eviction: no feasible plan for {} endpoints, requestId={}",
                    feasibilities.size(), ctx.getRequestId());
            return null;
        }

        boolean success = evictionPlanner.commit(best, ctx, roleType);
        if (!success) {
            Logger.warn("V4 eviction: commit failed, endpoint={}, requestId={}",
                    best.endpointIpPort(), ctx.getRequestId());
            return null;
        }

        // Build ServerStatus for the evicted endpoint
        WorkerEndpoint ep = endpointRegistry.get(roleType, best.endpointIpPort());
        if (ep == null) {
            Logger.warn("V4 eviction: endpoint not found after commit, ipPort={}", best.endpointIpPort());
            return null;
        }

        long seqLen = ctx.getRequest().getSeqLen();
        long maxNewTokens = ctx.getRequest().getMaxNewTokens();
        long expectedKvTokens = seqLen + maxNewTokens;
        long requestId = ctx.getRequestId();
        int priority = ctx.getPriority();

        if (ep instanceof DecodeEndpoint de) {
            Logger.info("V4 eviction succeeded: endpoint={}, cost={}, victims={}, requestId={}",
                    best.endpointIpPort(), best.cost(), best.victims().size(), requestId);
            return buildDecodeServerStatus(de, seqLen, expectedKvTokens, roleType, requestId, priority);
        } else if (ep instanceof PrefillEndpoint pe) {
            Logger.info("V4 eviction succeeded: endpoint={}, cost={}, victims={}, requestId={}",
                    best.endpointIpPort(), best.cost(), best.victims().size(), requestId);
            return buildPrefillServerStatus(pe, roleType, requestId);
        }

        return null;
    }

    // ==================== Priority Eviction: handleSelectFailure ====================

    /**
     * Handle select failure by attempting v4 cost-based eviction (primary) or
     * Phase 1 greedy eviction handlers (backward compatibility).
     *
     * <p>When {@code flexlbV4EvictionEnabled} is true (default):
     * <ul>
     *   <li>V4 cost-based eviction is the PRIMARY approach</li>
     *   <li>If v4 produces no feasible plan, returns a failure ServerStatus —
     *       does NOT fall through to old Phase 1 handlers</li>
     * </ul>
     *
     * <p>When {@code flexlbV4EvictionEnabled} is false:
     * <ul>
     *   <li>Uses old Phase 1 greedy eviction handlers (requires
     *       {@code flexlbPriorityEvictEnabled} and BATCH schedule mode)</li>
     * </ul>
     *
     * @return a success ServerStatus for the target EP (pendingHold), a failure
     *         ServerStatus if v4 is enabled but produced no feasible plan, or
     *         null if old Phase 1 handlers failed or are disabled
     */
    private ServerStatus handleSelectFailure(BalanceContext ctx, RoleType roleType,
                                              String group, ServerStatus failure) {
        FlexlbConfig config = ctx.getConfig();

        // V4 cost-based eviction is the PRIMARY approach when enabled.
        // If v4 produces no feasible plan, return failure — DO NOT fall through
        // to old Phase 1 greedy handlers.
        if (config != null && config.isFlexlbV4EvictionEnabled()) {
            ServerStatus v4Result = handleSelectFailureV4(ctx, roleType, group, failure);
            if (v4Result != null) {
                return v4Result;
            }
            // V4 eviction produced no feasible plan — fail immediately
            ServerStatus noFeasiblePlan = ServerStatus.code(NO_AVAILABLE_WORKER);
            noFeasiblePlan.setMessage("No feasible eviction plan");
            return noFeasiblePlan;
        }

        // Old Phase 1 greedy eviction handlers (backward compatibility when v4 is disabled)
        if (config == null || !config.isFlexlbPriorityEvictEnabled()) {
            return null;
        }
        if (ctx.getScheduleMode() != ScheduleModeEnum.BATCH) {
            return null;
        }

        Map<RejectionReason, Integer> rejections = failure.getRejections();
        if (rejections == null || rejections.isEmpty()) {
            return null;
        }

        int incomingPriority = ctx.getPriority();

        // Multi-handler fallback: try each rejection reason in eviction-cost priority order
        if (rejections.containsKey(RejectionReason.RESOURCE_UNAVAILABLE)) {
            ServerStatus result = handlePrefillPendingFull(ctx, roleType, group, incomingPriority, config);
            if (result != null) return result;
        }
        if (rejections.containsKey(RejectionReason.KV_CAPACITY) || rejections.containsKey(RejectionReason.KV_UNAVAILABLE)) {
            ServerStatus result = handleKvFull(ctx, roleType, group, incomingPriority);
            if (result != null) return result;
        }
        if (rejections.containsKey(RejectionReason.COMPUTE_SATURATED)) {
            ServerStatus result = handleComputeSaturated(ctx, roleType, group, incomingPriority);
            if (result != null) return result;
        }
        return null;
    }

    /**
     * Stage 2: Prefill batcher PQ eviction (PREFILL_PENDING_FULL).
     *
     * <p>Iterates prefill EPs, finds one with enough evictable low-priority queued
     * requests to bring realPendingCount below upperThreshold. Evicts victims from
     * PQ (no engine cancel) and directly assigns the target EP (pendingHold).
     */
    private ServerStatus handlePrefillPendingFull(BalanceContext ctx, RoleType roleType,
                                                    String group, int incomingPriority,
                                                    FlexlbConfig config) {
        long upperThreshold = config.getPrefillQueueSizeThreshold();

        PrefillEndpoint bestEp = null;
        List<org.flexlb.balance.scheduler.BatchItem> bestVictims = null;
        int minNeedEvict = Integer.MAX_VALUE;

        Map<String, WorkerEndpoint> endpoints = engineWorkerStatus.selectModelWorkerStatus(roleType, group);
        for (Map.Entry<String, WorkerEndpoint> entry : endpoints.entrySet()) {
            if (!(entry.getValue() instanceof PrefillEndpoint pe)) {
                continue;
            }
            if (!pe.getStatus().isAlive()) {
                continue;
            }

            long realPending = pe.realPendingCount();
            int needEvict = (int) Math.max(0, realPending - upperThreshold + 1);
            if (needEvict <= 0) {
                continue;
            }

            List<org.flexlb.balance.scheduler.BatchItem> victims =
                    pe.findEvictableQueuedRequests(incomingPriority, needEvict);
            if (victims.size() >= needEvict && needEvict < minNeedEvict) {
                bestEp = pe;
                bestVictims = victims;
                minNeedEvict = needEvict;
            }
        }

        if (bestEp == null) {
            Logger.warn("Stage 2 prefill eviction failed: no EP can cover, incomingPriority={}", incomingPriority);
            return null;
        }

        Logger.warn("Stage 2 prefill eviction: ep={}, victims={}, needEvict={}, incomingPriority={}",
                bestEp.ipPort(), bestVictims.size(), minNeedEvict, incomingPriority);

        // Evict victims (remove from PQ + cleanup via handler.onOfferFailure)
        bestEp.evictQueuedItems(bestVictims);
        reportEviction("PREFILL_PENDING_FULL", bestVictims.size());

        // pendingHold: directly assign target EP (no re-select)
        return buildPrefillServerStatus(bestEp, roleType, ctx.getRequestId());
    }

    /**
     * Stage 3a: Decode KV_FULL eviction.
     *
     * <p>Two-pass: pass1 tries unconfirmed victims (inflightRequests, no engine cancel),
     * pass2 tries running victims (runningInflightRequests, engine cancel).
     * Selects the EP with the fewest victims that can cover needEvictKvTokens.
     */
    private ServerStatus handleKvFull(BalanceContext ctx, RoleType roleType,
                                       String group, int incomingPriority) {
        long seqLen = ctx.getRequest().getSeqLen();
        long maxNewTokens = ctx.getRequest().getMaxNewTokens();
        long expectedKvTokens = seqLen + maxNewTokens;
        long requestId = ctx.getRequestId();

        Map<String, WorkerEndpoint> endpoints = engineWorkerStatus.selectModelWorkerStatus(roleType, group);

        // pass1: unconfirmed victims
        DecodeEndpoint bestEp = null;
        List<EvictionVictim> bestVictims = null;
        int minVictimCount = Integer.MAX_VALUE;

        for (Map.Entry<String, WorkerEndpoint> entry : endpoints.entrySet()) {
            if (!(entry.getValue() instanceof DecodeEndpoint de)) {
                continue;
            }
            if (!de.getStatus().isAlive()) {
                continue;
            }
            List<EvictionVictim> victims = de.findUnconfirmedEvictableVictims(incomingPriority, seqLen);
            if (victims.isEmpty()) {
                continue;
            }
            long freed = victims.stream().mapToLong(EvictionVictim::kvTokens).sum();
            if (freed >= seqLen && victims.size() < minVictimCount) {
                bestEp = de;
                bestVictims = victims;
                minVictimCount = victims.size();
            }
        }

        if (bestEp != null) {
            Logger.warn("Stage 3a KV_FULL eviction (unconfirmed): ep={}, victims={}, freed>={}, seqLen={}",
                    bestEp.ipPort(), bestVictims.size(), seqLen, seqLen);
            for (EvictionVictim v : bestVictims) {
                bestEp.removeUnconfirmedVictim(v.requestId());
            }
            reportEviction("KV_FULL", bestVictims.size());
            return buildDecodeServerStatus(bestEp, seqLen, expectedKvTokens, roleType, requestId, incomingPriority);
        }

        // pass2: running victims
        for (Map.Entry<String, WorkerEndpoint> entry : endpoints.entrySet()) {
            if (!(entry.getValue() instanceof DecodeEndpoint de)) {
                continue;
            }
            if (!de.getStatus().isAlive()) {
                continue;
            }
            List<EvictionVictim> victims = de.findRunningEvictableVictims(incomingPriority, seqLen);
            if (victims.isEmpty()) {
                continue;
            }
            long freed = victims.stream().mapToLong(EvictionVictim::kvTokens).sum();
            if (freed >= seqLen && victims.size() < minVictimCount) {
                bestEp = de;
                bestVictims = victims;
                minVictimCount = victims.size();
            }
        }

        if (bestEp != null) {
            Logger.warn("Stage 3a KV_FULL eviction (running): ep={}, victims={}, needKv={}, incomingPriority={}",
                    bestEp.ipPort(), bestVictims.size(), seqLen, incomingPriority);
            for (EvictionVictim v : bestVictims) {
                bestEp.removeRunningVictim(v.requestId());
                callCancel(v.requestId());
            }
            reportEviction("KV_FULL", bestVictims.size());
            return buildDecodeServerStatus(bestEp, seqLen, expectedKvTokens, roleType, requestId, incomingPriority);
        }

        Logger.warn("Stage 3a KV_FULL eviction failed: no EP can cover, seqLen={}, incomingPriority={}",
                seqLen, incomingPriority);
        return null;
    }

    /**
     * Stage 3b: Decode COMPUTE_SATURATED eviction.
     *
     * <p>Two-pass: pass1 tries unconfirmed concurrency victim, pass2 tries running.
     * Only one victim is needed to free one concurrency slot.
     */
    private ServerStatus handleComputeSaturated(BalanceContext ctx, RoleType roleType,
                                                  String group, int incomingPriority) {
        long seqLen = ctx.getRequest().getSeqLen();
        long maxNewTokens = ctx.getRequest().getMaxNewTokens();
        long expectedKvTokens = seqLen + maxNewTokens;
        long requestId = ctx.getRequestId();

        Map<String, WorkerEndpoint> endpoints = engineWorkerStatus.selectModelWorkerStatus(roleType, group);

        // pass1: unconfirmed
        for (Map.Entry<String, WorkerEndpoint> entry : endpoints.entrySet()) {
            if (!(entry.getValue() instanceof DecodeEndpoint de)) {
                continue;
            }
            if (!de.getStatus().isAlive()) {
                continue;
            }
            EvictionVictim victim = de.findUnconfirmedEvictableConcurrencyVictim(incomingPriority);
            if (victim != null) {
                Logger.warn("Stage 3b COMPUTE_SATURATED eviction (unconfirmed): ep={}, victim={}, incomingPriority={}",
                        de.ipPort(), victim.requestId(), incomingPriority);
                de.removeUnconfirmedVictim(victim.requestId());
                reportEviction("COMPUTE_SATURATED", 1);
                return buildDecodeServerStatus(de, seqLen, expectedKvTokens, roleType, requestId, incomingPriority);
            }
        }

        // pass2: running
        for (Map.Entry<String, WorkerEndpoint> entry : endpoints.entrySet()) {
            if (!(entry.getValue() instanceof DecodeEndpoint de)) {
                continue;
            }
            if (!de.getStatus().isAlive()) {
                continue;
            }
            EvictionVictim victim = de.findRunningEvictableConcurrencyVictim(incomingPriority);
            if (victim != null) {
                Logger.warn("Stage 3b COMPUTE_SATURATED eviction (running): ep={}, victim={}, incomingPriority={}",
                        de.ipPort(), victim.requestId(), incomingPriority);
                de.removeRunningVictim(victim.requestId());
                callCancel(victim.requestId());
                reportEviction("COMPUTE_SATURATED", 1);
                return buildDecodeServerStatus(de, seqLen, expectedKvTokens, roleType, requestId, incomingPriority);
            }
        }

        Logger.warn("Stage 3b COMPUTE_SATURATED eviction failed: no victim found, incomingPriority={}",
                incomingPriority);
        return null;
    }

    // ==================== PendingHold: direct EP assignment ====================

    /**
     * Build a success ServerStatus for a decode EP and reserve KV tokens (pendingHold).
     * Does NOT re-run select — the EP was chosen by eviction, not by the strategy.
     */
    private ServerStatus buildDecodeServerStatus(DecodeEndpoint ep, long seqLen,
                                                  long expectedKvTokens, RoleType roleType,
                                                  long requestId, int priority) {
        long totalKv = ep.realKvTotal();
        if (totalKv > 0 && expectedKvTokens > totalKv) {
            expectedKvTokens = totalKv;
        }
        ep.reserve(requestId, seqLen, expectedKvTokens, priority);

        ServerStatus result = new ServerStatus();
        result.setSuccess(true);
        result.setRole(roleType);
        result.setServerIp(ep.getIp());
        result.setHttpPort(ep.getHttpPort());
        result.setGrpcPort(CommonUtils.toGrpcPort(ep.getHttpPort()));
        result.setDpRank(ep.getStatus().getDpRank());
        result.setGroup(ep.getStatus().getGroup());
        result.setRequestId(requestId);
        return result;
    }

    /**
     * Build a success ServerStatus for a prefill EP (pendingHold).
     * No reserve needed for batch path — the batcher will handle inflight tracking.
     */
    private ServerStatus buildPrefillServerStatus(PrefillEndpoint ep, RoleType roleType, long requestId) {
        ServerStatus result = new ServerStatus();
        result.setSuccess(true);
        result.setRole(roleType);
        result.setRequestId(requestId);
        result.setGroup(ep.getStatus().getGroup());
        result.setServerIp(ep.getIp());
        result.setHttpPort(ep.getHttpPort());
        result.setGrpcPort(CommonUtils.toGrpcPort(ep.getHttpPort()));
        result.setDpRank(ep.getStatus().getDpRank());

        DebugInfo debugInfo = new DebugInfo();
        debugInfo.setHitCacheLen(0);
        result.setDebugInfo(debugInfo);
        return result;
    }

    /**
     * Report a priority eviction event to the metrics reporter.
     */
    private void reportEviction(String stage, int victimCount) {
        BatchSchedulerReporter reporter = reporterProvider.getIfAvailable();
        if (reporter != null) {
            reporter.reportPriorityEvict(stage, victimCount);
        }
    }

    /**
     * Call the cancel handler for a running victim (engine cancel).
     * Dispatches cancel RPC via CancelHandler with PRIORITY_PREEMPTED reason.
     */
    private void callCancel(long requestId) {
        CancelHandler handler = cancelHandlerProvider.getIfAvailable();
        if (handler != null) {
            handler.cancel(requestId, CancelReasonPB.CANCEL_REASON_PRIORITY_PREEMPTED);
        } else {
            Logger.warn("CancelHandler not available, skipping cancel for requestId={}", requestId);
        }
        BatchSchedulerReporter reporter = reporterProvider.getIfAvailable();
        if (reporter != null) {
            reporter.reportPriorityCancel("router");
        }
    }

    // ==================== Rollback ====================

    private void rollBackRoutingFailure(BalanceContext balanceContext, RoutingResult routingResult) {
        List<ServerStatus> partialResults = routingResult.serverStatusList();
        for (ServerStatus serverStatus : partialResults) {
            String serverIpPort = serverStatus.getServerIp() + ":" + serverStatus.getHttpPort();
            long requestId = balanceContext.getRequestId();
            RoleType role = serverStatus.getRole();

            WorkerEndpoint ep = endpointRegistry.get(role, serverIpPort);
            if (ep == null) {
                Logger.warn("DefaultRouter.rollBack: endpoint not found for ipPort={}", serverIpPort);
                continue;
            }

            LoadBalanceStrategy loadBalanceStrategy = getLoadBalanceStrategy(role);
            loadBalanceStrategy.rollBack(ep, requestId);
        }
    }

    private Response buildSuccessResponse(List<ServerStatus> serverStatusList) {
        Response response = new Response();
        response.setSuccess(true);
        response.setServerStatus(serverStatusList);
        return response;
    }

    private Response buildFailureResponse(RoutingResult routingResult) {
        StrategyErrorType errorType = routingResult.failedRoleType().getErrorType();
        String detailMessage = routingResult.errorMessage();

        Response response = new Response();
        response.setSuccess(false);
        response.setCode(errorType.getErrorCode());
        response.setErrorMessage(errorType.getErrorMsg() + ": " + detailMessage);
        return response;
    }
}
