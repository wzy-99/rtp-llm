package org.flexlb.httpserver;

import io.grpc.stub.StreamObserver;
import org.flexlb.consistency.LBStatusConsistencyService;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Request;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;
import org.flexlb.engine.grpc.EngineRpcService;
import org.flexlb.engine.grpc.FlexlbServiceGrpc;
import org.flexlb.engine.grpc.RoleTypeProtoConverter;
import org.flexlb.enums.ScheduleModeEnum;
import org.flexlb.service.CancelRouter;
import org.flexlb.service.RouteService;
import org.flexlb.service.grace.ActiveRequestCounter;
import org.flexlb.service.monitor.EngineHealthReporter;
import org.flexlb.config.ConfigService;
import org.flexlb.config.FlexlbConfig;
import org.flexlb.util.Logger;
import org.springframework.stereotype.Component;

@Component
public class FlexlbServiceImpl extends FlexlbServiceGrpc.FlexlbServiceImplBase {

    private final RouteService routeService;
    private final CancelRouter cancelRouter;
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
                             ConfigService configService,
                             CancelRouter cancelRouter) {
        this.routeService = routeService;
        this.cancelRouter = cancelRouter;
        this.lbStatusConsistencyService = lbStatusConsistencyService;
        this.engineHealthReporter = engineHealthReporter;
        this.activeRequestCounter = activeRequestCounter;
        this.grpcForwarder = grpcForwarder;
        this.configService = configService;
    }

    @Override
    public void schedule(EngineRpcService.FlexlbScheduleRequestPB request,
                         StreamObserver<EngineRpcService.FlexlbScheduleResponsePB> responseObserver) {
        ActiveRequestCounter.RequestToken token = activeRequestCounter.acquire();
        try {
            FlexlbRequest flexlbRequest = buildRequest(request);
            engineHealthReporter.reportArriveDelayTime(flexlbRequest);

            EngineRpcService.FlexlbScheduleResponsePB response;
            if (lbStatusConsistencyService.isNeedConsistency() && !lbStatusConsistencyService.isMaster()) {
                response = grpcForwarder.forwardToMaster(request);
                if (response == null) {
                    response = routeLocally(flexlbRequest);
                }
            } else {
                response = routeLocally(flexlbRequest);
            }

            responseObserver.onNext(response);
            responseObserver.onCompleted();
            engineHealthReporter.reportBalancingService(flexlbRequest);
        } catch (Exception e) {
            Logger.error("FlexlbService.schedule error, request_id={}", request.getRequestId(), e);
            EngineRpcService.FlexlbScheduleResponsePB errorResp = EngineRpcService.FlexlbScheduleResponsePB.newBuilder()
                    .setSuccess(false)
                    .setCode(500)
                    .setErrorMessage(e.getMessage() != null ? e.getMessage() : "internal error")
                    .build();
            responseObserver.onNext(errorResp);
            responseObserver.onCompleted();
        } finally {
            token.close();
        }
    }

    @Override
    public void cancel(EngineRpcService.CancelRequestPB request,
                       StreamObserver<EngineRpcService.EmptyPB> responseObserver) {
        try {
            cancelRouter.cancel(request.getRequestId());
            responseObserver.onNext(EngineRpcService.EmptyPB.getDefaultInstance());
            responseObserver.onCompleted();
        } catch (Exception e) {
            Logger.error("FlexlbService.cancel error, request_id={}", request.getRequestId(), e);
            responseObserver.onError(io.grpc.Status.INTERNAL
                    .withDescription(e.getMessage())
                    .asRuntimeException());
        }
    }

    private EngineRpcService.FlexlbScheduleResponsePB routeLocally(FlexlbRequest request) {
        Response response = routeService.route(request).block();
        return toProtoResponse(response);
    }

    private FlexlbRequest buildRequest(EngineRpcService.FlexlbScheduleRequestPB pb) {
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

        FlexlbRequest flexlbRequest = new FlexlbRequest(request);

        if (pb.hasGenerateInput()) {
            flexlbRequest.setGenerateInputPbBytes(pb.getGenerateInput().toByteArray());
        }

        flexlbRequest.setScheduleMode(resolveScheduleMode(pb.getScheduleMode(), configService.loadBalanceConfig()));
        return flexlbRequest;
    }

    private static ScheduleModeEnum resolveScheduleMode(EngineRpcService.FlexlbScheduleModePB mode,
                                                        FlexlbConfig config) {
        return switch (mode) {
            case FLEXLB_SCHEDULE_BATCH -> ScheduleModeEnum.BATCH;
            case FLEXLB_SCHEDULE_DIRECT -> ScheduleModeEnum.DIRECT;
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
                        .setRole(RoleTypeProtoConverter.toProto(ss.getRole()))
                        .setServerIp(ss.getServerIp() != null ? ss.getServerIp() : "")
                        .setHttpPort(ss.getHttpPort())
                        .setGrpcPort(ss.getGrpcPort())
                        .build());
            }
        }
        return builder.build();
    }
}
