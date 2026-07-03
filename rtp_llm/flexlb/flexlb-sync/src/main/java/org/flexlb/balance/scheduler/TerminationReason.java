package org.flexlb.balance.scheduler;

import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.StrategyErrorType;

/**
 * Categorises every termination scenario for inflight batch requests.
 *
 * <p>Each reason carries two behavioural flags:
 * <ul>
 *   <li>{@code cancelLike} — if true, {@code terminate()} calls {@code item.cancel()}
 *       to set the cancelled flag, enabling batcher/dispatcher filtering.</li>
 *   <li>{@code requiresEngineCancel} — if true, {@code terminate()} sends a gRPC
 *       cancel to the prefill engine to interrupt the in-flight request.</li>
 * </ul>
 */
public enum TerminationReason {

    CANCELLED       (true,  true,  StrategyErrorType.REQUEST_CANCELLED,    "Request cancelled by client"),
    SLO_EXPIRED     (false, false, StrategyErrorType.BATCH_SLO_EXPIRED,    "FlexLB request deadline expired"),
    OFFER_FAILED    (false, false, StrategyErrorType.BATCH_DISPATCH_FAILED,"Batcher offer failed"),
    DISPATCH_FAILED (false, false, StrategyErrorType.BATCH_DISPATCH_FAILED,"gRPC dispatch failed"),
    TTL_EXPIRED     (false, false, StrategyErrorType.REQUEST_CANCELLED,    "Request TTL expired");

    private final boolean cancelLike;
    private final boolean requiresEngineCancel;
    private final StrategyErrorType errorType;
    private final String defaultMessage;

    TerminationReason(boolean cancelLike, boolean requiresEngineCancel,
                      StrategyErrorType errorType, String defaultMessage) {
        this.cancelLike = cancelLike;
        this.requiresEngineCancel = requiresEngineCancel;
        this.errorType = errorType;
        this.defaultMessage = defaultMessage;
    }

    public boolean isCancelLike() {
        return cancelLike;
    }

    public boolean requiresEngineCancel() {
        return requiresEngineCancel;
    }

    public StrategyErrorType errorType() {
        return errorType;
    }

    public String defaultMessage() {
        return defaultMessage;
    }

    /**
     * Build an error {@link Response} for this termination reason using the
     * default message.
     */
    public Response toResponse() {
        return toResponse(defaultMessage);
    }

    /**
     * Build an error {@link Response} for this termination reason using a
     * caller-supplied detail string (e.g. exception message).
     */
    public Response toResponse(String detail) {
        Response resp = new Response();
        resp.setSuccess(false);
        resp.setCode(errorType.getErrorCode());
        resp.setErrorMessage(detail);
        return resp;
    }
}
