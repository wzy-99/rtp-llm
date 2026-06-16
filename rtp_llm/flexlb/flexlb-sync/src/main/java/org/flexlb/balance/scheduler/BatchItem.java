package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.dao.BalanceContext;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;

import java.util.concurrent.CompletableFuture;

/**
 * A single inference request queued for batch dispatch.
 *
 * <p>Extracted from {@link FlexlbBatchScheduler} to reduce coupling
 * with {@link WorkerBatcher}.
 *
 * <p>Carries direct {@link PrefillEndpoint} / {@link DecodeEndpoint} references
 * so downstream operations (commit, rollback, ack, cancel) avoid repeated
 * {@code EndpointRegistry} lookups by ip+port.
 */
public record BatchItem(BalanceContext ctx,
                         CompletableFuture<Response> future,
                         Response routeResponse,
                         ServerStatus prefill,
                         ServerStatus decode,
                         PrefillEndpoint prefillEp,
                         DecodeEndpoint decodeEp,
                         long deadlineMs,
                         long enqueuedAtMs) {

    public long requestId() {
        return ctx != null && ctx.getRequest() != null
                ? ctx.getRequest().getRequestId() : 0;
    }

    /** Total sequence length of this request. */
    public long seqLen() {
        return ctx != null && ctx.getRequest() != null
                ? ctx.getRequest().getSeqLen() : 0;
    }

    /** Cache-hit tokens on the assigned prefill endpoint. */
    public long hitCache() {
        return hitCacheOf(prefill);
    }

    /** Compute tokens = seqLen - hitCache (floor at 0). */
    public long computeTokens() {
        return Math.max(0, seqLen() - hitCache());
    }

    /** Extract cache-hit length from a {@link ServerStatus} debug info. */
    public static long hitCacheOf(ServerStatus ss) {
        return ss != null && ss.getDebugInfo() != null
                ? ss.getDebugInfo().getHitCacheLen() : 0;
    }
}
