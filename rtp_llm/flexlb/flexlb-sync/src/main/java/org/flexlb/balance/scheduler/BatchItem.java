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
 * so downstream operations (commit, rollback, ack) avoid repeated
 * {@code EndpointRegistry} lookups by ip+port.
 *
 * <p>{@link #sortKey} is mutable — the {@link BatcherAlgorithm} computes it
 * inside {@link WorkerBatcher#offer(BatchItem)} via {@link BatcherAlgorithm#computeSortKey}.
 */
public final class BatchItem {

    private final BalanceContext ctx;
    private final CompletableFuture<Response> future;
    private final Response routeResponse;
    private final ServerStatus prefill;
    private final ServerStatus decode;
    private final PrefillEndpoint prefillEp;
    private final DecodeEndpoint decodeEp;
    private final long enqueuedAtMs;

    /** Mutable sort key set by the batcher algorithm at offer time. */
    private volatile long sortKey;

    /**
     * Request priority (30/40/50/60/70, default 50).
     * Initialized from {@link BalanceContext#getRequest()} at construction time;
     * may be updated by downstream logic (e.g. eviction).
     */
    private volatile int priority = 50;

    /**
     * SLO deadline (absolute epoch ms) computed by the batcher algorithm
     * in {@link BatcherAlgorithm#computeSortKey}. Stored so the transfer
     * scan can check danger-zone eligibility without re-deriving it from
     * the sort key.
     */
    private volatile long deadline;

    /**
     * Number of times this request has been transferred between batchers.
     * Incremented each time the SLO transfer logic re-routes the request.
     * Capped by {@code FlexlbConfig.sloTransferMaxCount} to prevent storms.
     */
    private volatile int transferCount;

    public BatchItem(BalanceContext ctx,
                     CompletableFuture<Response> future,
                     Response routeResponse,
                     ServerStatus prefill,
                     ServerStatus decode,
                     PrefillEndpoint prefillEp,
                     DecodeEndpoint decodeEp,
                     long enqueuedAtMs) {
        this.ctx = ctx;
        this.future = future;
        this.routeResponse = routeResponse;
        this.prefill = prefill;
        this.decode = decode;
        this.prefillEp = prefillEp;
        this.decodeEp = decodeEp;
        this.enqueuedAtMs = enqueuedAtMs;
        if (ctx != null && ctx.getRequest() != null) {
            this.priority = ctx.getRequest().getPriority();
        }
    }

    // -- accessors --

    public BalanceContext ctx() { return ctx; }
    public CompletableFuture<Response> future() { return future; }
    public Response routeResponse() { return routeResponse; }
    public ServerStatus prefill() { return prefill; }
    public ServerStatus decode() { return decode; }
    public PrefillEndpoint prefillEp() { return prefillEp; }
    public DecodeEndpoint decodeEp() { return decodeEp; }
    public long enqueuedAtMs() { return enqueuedAtMs; }

    /** Priority queue sort key. */
    public long sortKey() { return sortKey; }

    /** Set by {@link WorkerBatcher#offer} after {@link BatcherAlgorithm#computeSortKey}. */
    public void setSortKey(long sortKey) { this.sortKey = sortKey; }

    /** Request priority (higher = more urgent). */
    public int priority() { return priority; }

    /** Update priority (e.g. for eviction scenarios). */
    public void setPriority(int priority) { this.priority = priority; }

    /** SLO deadline (absolute epoch ms), set during sort-key computation. */
    public long deadline() { return deadline; }

    /** Set by {@link BatcherAlgorithm#computeSortKey} alongside the sort key. */
    public void setDeadline(long deadline) { this.deadline = deadline; }

    /** Number of times this request has been transferred between batchers. */
    public int getTransferCount() { return transferCount; }

    /** Set the transfer count (used when carrying over to a re-routed item). */
    public void setTransferCount(int transferCount) { this.transferCount = transferCount; }

    /** Increment the transfer count by one. */
    public void incrementTransferCount() { this.transferCount++; }

    // -- derived accessors --

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

    /** Extract cache-hit length from a {@link ServerStatus} debug info. */
    private static long hitCacheOf(ServerStatus ss) {
        return ss != null && ss.getDebugInfo() != null
                ? ss.getDebugInfo().getHitCacheLen() : 0;
    }

}
