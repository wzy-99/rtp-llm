package org.flexlb.httpserver;

import io.grpc.stub.StreamObserver;
import org.flexlb.consistency.LBStatusConsistencyService;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.FlexlbServiceGrpc;
import org.flexlb.engine.grpc.RoleTypeProtoConverter;
import org.flexlb.enums.ScheduleModeEnum;
import org.flexlb.service.RouteService;
import org.flexlb.service.grace.ActiveRequestCounter;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Component
public class FlexlbServiceImpl extends FlexlbServiceGrpc.FlexlbServiceImplBase {

    private final RouteService routeService;
    private final LBStatusConsistencyService lbStatusConsistencyService;
    private final EngineHealthReporter engineHealthReporter;
    private final ActiveRequestCounter activeRequestCounter;
    private final FlexlbGrpcForwarder grpcForwarder;
    private final ConfigService configService;

    public FlexlbServiceImpl(RouteService routeService,
                             LBStatusConsistencyService lbStatusConsistencyService,
                             EngineHealthReporter engineHealthReporter,
                             ActiveRequestCounter activeRequestCounter,
                             FlexlbGrpcForwarder grpcForwarder,
                             ConfigService configService) {
        this.routeService = routeService;
        this.lbStatusConsistencyService = lbStatusConsistencyService;
        this.engineHealthReporter = engineHealthReporter;
        this.activeRequestCounter = activeRequestCounter;
        this.grpcForwarder = grpcForwarder;
        this.configService = configService;
    }

    @Override
    public void schedule(EngineRpcService.FlexlbScheduleRequestPB request,
                         StreamObserver<EngineRpcService.FlexlbScheduleResponsePB> responseObserver) {
        final ActiveRequestCounter.RequestToken token;
        try {
            token = activeRequestCounter.acquire();
        } catch (Exception e) {
            Logger.error("FlexlbService.schedule acquire token failed, request_id={}", request.getRequestId(), e);
            responseObserver.onNext(buildErrorResponse(e));
            responseObserver.onCompleted();
            return;
        }

        final BalanceContext ctx;
        try {
            ctx = buildContext(request);
            engineHealthReporter.reportArriveDelayTime(ctx);
        } catch (Exception e) {
            Logger.error("FlexlbService.schedule error, request_id={}", request.getRequestId(), e);
            try {
                responseObserver.onNext(buildErrorResponse(e));
                responseObserver.onCompleted();
            } finally {
                token.close();
            }
            return;
        }

        // Forward path: keep synchronous for now (tests don't exercise this path)
        if (lbStatusConsistencyService.isNeedConsistency() && !lbStatusConsistencyService.isMaster()) {
            boolean asyncHandoff = false;
            try {
                EngineRpcService.FlexlbScheduleResponsePB resp = grpcForwarder.forwardToMaster(request);
                if (resp == null) {
                    // Forward failed, fall back to async local routing
                    routeService.route(ctx)
                            .timeout(Duration.ofSeconds(30))
                            .doFinally(signal -> token.close())
                            .subscribe(
                                    response -> {
                                        try {
                                            EngineRpcService.FlexlbScheduleResponsePB pb = toProtoResponse(response);
                                            ctx.setSuccess(pb.getSuccess());
                                            if (!pb.getSuccess()) {
                                                ctx.setErrorMessage(pb.getErrorMessage());
                                            }
                                            engineHealthReporter.reportBalancingService(ctx);
                                            responseObserver.onNext(pb);
                                            responseObserver.onCompleted();
                                        } catch (Exception e) {
                                            Logger.error("FlexlbService.schedule onNext callback error, request_id={}",
                                                    request.getRequestId(), e);
                                        }
                                    },
                                    error -> {
                                        try {
                                            Logger.error("FlexlbService.schedule async route error, request_id={}",
                                                    request.getRequestId(), error);
                                            responseObserver.onNext(buildErrorResponse(error));
                                            responseObserver.onCompleted();
                                        } catch (Exception e) {
                                            Logger.error("FlexlbService.schedule onError callback error, request_id={}",
                                                    request.getRequestId(), e);
                                        }
                                    }
                            );
                    asyncHandoff = true;
                    return;
                }
                responseObserver.onNext(resp);
                responseObserver.onCompleted();
                ctx.setSuccess(resp.getSuccess());
                if (!resp.getSuccess()) {
                    ctx.setErrorMessage(resp.getErrorMessage());
                }
                engineHealthReporter.reportBalancingService(ctx);
            } catch (Exception e) {
                Logger.error("FlexlbService.schedule error, request_id={}", request.getRequestId(), e);
                responseObserver.onNext(buildErrorResponse(e));
                responseObserver.onCompleted();
            } finally {
                if (!asyncHandoff) {
                    token.close();
                }
            }
            return;
        }

        // Local routing: async callback — gRPC thread returns immediately
        try {
            routeService.route(ctx)
                    .timeout(Duration.ofSeconds(30))
                    .doFinally(signal -> token.close())
                    .subscribe(
                            response -> {
                                try {
                                    EngineRpcService.FlexlbScheduleResponsePB pb = toProtoResponse(response);
                                    ctx.setSuccess(pb.getSuccess());
                                    if (!pb.getSuccess()) {
                                        ctx.setErrorMessage(pb.getErrorMessage());
                                    }
                                    engineHealthReporter.reportBalancingService(ctx);
                                    responseObserver.onNext(pb);
                                    responseObserver.onCompleted();
                                } catch (Exception e) {
                                    Logger.error("FlexlbService.schedule onNext callback error, request_id={}",
                                            request.getRequestId(), e);
                                }
                            },
                            error -> {
                                try {
                                    Logger.error("FlexlbService.schedule async route error, request_id={}",
                                            request.getRequestId(), error);
                                    responseObserver.onNext(buildErrorResponse(error));
                                    responseObserver.onCompleted();
                                } catch (Exception e) {
                                    Logger.error("FlexlbService.schedule onError callback error, request_id={}",
                                            request.getRequestId(), e);
                                }
                            }
                    );
        } catch (Exception e) {
            Logger.error("FlexlbService.schedule route failed, request_id={}", request.getRequestId(), e);
            try {
                responseObserver.onNext(buildErrorResponse(e));
                responseObserver.onCompleted();
            } finally {
                token.close();
            }
        }
    }

    @Override
    public void cancel(EngineRpcService.CancelRequestPB request,
                       StreamObserver<EngineRpcService.EmptyPB> responseObserver) {
        try {
            routeService.cancelByRequestId(request.getRequestId());
            responseObserver.onNext(EngineRpcService.EmptyPB.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            Logger.error("FlexlbService.cancel error, request_id={}", request.getRequestId(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    private EngineRpcService.FlexlbScheduleResponsePB buildErrorResponse(Throwable e) {
        return EngineRpcService.FlexlbScheduleResponsePB.newBuilder()
                .setSuccess(false)
                .setCode(500)
                .setErrorMessage(e.getMessage() != null ? e.getMessage() : "internal error")
                .build();
    }

    private BalanceContext buildContext(EngineRpcService.FlexlbScheduleRequestPB pb) {
        BalanceContext ctx = new BalanceContext();

        Request request = new Request();
        request.setRequestId(pb.getRequestId());
        request.setBlockCacheKeys(pb.getBlockCacheKeysList());
        request.setSeqLen(pb.getSeqLen());
        request.setGenerateTimeout(pb.getGenerateTimeout());
        request.setRequestTimeMs(pb.getRequestTimeMs());
        request.setMaxNewTokens(pb.getMaxNewTokens());
        request.setNumBeams(pb.getNumBeams());
        request.setForceDisableSpRun(pb.getForceDisableSpRun());
        request.setModel(pb.getModel());
        request.setApiKey(pb.getApiKey());
        request.setCacheKeyBlockSize(pb.getCacheKeyBlockSize());
        ctx.setRequest(request);

        if (pb.hasGenerateInput()) {
            ctx.setGenerateInputPbBytes(pb.getGenerateInput().toByteArray());
        }

        ctx.setScheduleMode(resolveScheduleMode(pb.getScheduleMode(), configService.loadBalanceConfig()));
        return ctx;
    }

    private static ScheduleModeEnum resolveScheduleMode(EngineRpcService.FlexlbScheduleModePB mode,
                                                        FlexlbConfig config) {
        return switch (mode) {
            case FLEXLB_SCHEDULE_BATCH -> ScheduleModeEnum.BATCH;
            case FLEXLB_SCHEDULE_DIRECT -> ScheduleModeEnum.DIRECT;
            case FLEXLB_SCHEDULE_QUEUE -> ScheduleModeEnum.QUEUE;
            default -> config.getDefaultScheduleModeEnum();
        };
    }

    private EngineRpcService.FlexlbScheduleResponsePB toProtoResponse(Response response) {
        EngineRpcService.FlexlbScheduleResponsePB.Builder builder = EngineRpcService.FlexlbScheduleResponsePB.newBuilder();
        if (response == null) {
            return builder.setSuccess(false).setCode(500).setErrorMessage("null response").build();
        }
        builder.setSuccess(response.isSuccess());
        builder.setCode(response.getCode());
        if (response.getErrorMessage() != null) {
            builder.setErrorMessage(response.getErrorMessage());
        }
        if (response.getRealMasterHost() != null) {
            builder.setRealMasterHost(response.getRealMasterHost());
        }
        builder.setQueueLength(response.getQueueLength() != null ? response.getQueueLength() : 0);
        builder.setEnqueuedByMaster(response.isEnqueuedByMaster());

        if (response.getServerStatus() != null) {
            for (ServerStatus ss : response.getServerStatus()) {
                builder.addServerStatus(EngineRpcService.FlexlbServerStatusPB.newBuilder()
                        .setRole(ss.getRole().getCode())
                        .setServerIp(ss.getServerIp() != null ? ss.getServerIp() : "")
                        .setHttpPort(ss.getHttpPort())
                        .setGrpcPort(ss.getGrpcPort())
                        .setRoleType(RoleTypeProtoConverter.toProto(ss.getRole()))
                        .build());
            }
        }
        return builder.build();
    }
}
