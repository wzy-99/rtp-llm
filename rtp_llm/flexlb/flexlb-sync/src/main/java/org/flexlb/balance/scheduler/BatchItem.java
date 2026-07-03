package org.flexlb.balance.scheduler;

import org.flexlb.balance.endpoint.DecodeEndpoint;
import org.flexlb.balance.endpoint.PrefillEndpoint;
import org.flexlb.dao.FlexlbRequest;
import org.flexlb.dao.loadbalance.Response;
import org.flexlb.dao.loadbalance.ServerStatus;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A single inference request queued for batch dispatch.
 *
 * <p>Carries direct {@link PrefillEndpoint} / {@link DecodeEndpoint} references
 * so downstream operations (commit, rollback, ack, cancel) avoid repeated
 * {@code EndpointRegistry} lookups by ip+port.
 *
 * <p>Also absorbs the former {@code InflightEntry} fields (rolledBack,
 * ackFinished, batchId, terminated) to eliminate the three-layer wrapping
 * (BalanceContext → BatchItem → InflightEntry).
 */
public final class BatchItem implements InflightEvictor.TtlTracked {

    private final FlexlbRequest request;
    private final Response routeResponse;
    private final ServerStatus prefill;
    private final ServerStatus decode;
    private final PrefillEndpoint prefillEp;
    private final DecodeEndpoint decodeEp;
    private final long enqueuedAtMs;

    /** Mutable sort key set by the batcher algorithm at offer time. */
    private volatile long sortKey;

    // --- Former InflightEntry fields (absorbed) ---

    private final AtomicBoolean rolledBack = new AtomicBoolean(false);
    private volatile boolean ackFinished;
    private volatile long batchId = -1;
    private final AtomicBoolean terminated = new AtomicBoolean(false);

    public BatchItem(FlexlbRequest request,
                     Response routeResponse,
                     ServerStatus prefill,
                     ServerStatus decode,
                     PrefillEndpoint prefillEp,
                     DecodeEndpoint decodeEp,
                     long sortKey,
                     long enqueuedAtMs) {
        this.request = request;
        this.routeResponse = routeResponse;
        this.prefill = prefill;
        this.decode = decode;
        this.prefillEp = prefillEp;
        this.decodeEp = decodeEp;
        this.sortKey = sortKey;
        this.enqueuedAtMs = enqueuedAtMs;
    }

    // -- accessors --

    public FlexlbRequest request() { return request; }
    public CompletableFuture<Response> future() { return request.getFuture(); }
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

    /** @deprecated use {@link #sortKey()} instead; kept for SLO-budget references. */
    @Deprecated
    public long deadlineMs() { return sortKey; }

    // -- TtlTracked --

    @Override
    public long createdAtMs() { return enqueuedAtMs; }

    // -- delegation methods (eliminate multi-layer indirection) --

    public long requestId() {
        return request != null ? request.getRequestId() : 0;
    }

    public boolean isCancelled() {
        return request != null && request.isCancelled();
    }

    public void cancel() {
        if (request != null) {
            request.cancel();
        }
    }

    public boolean tryTerminate() {
        return terminated.compareAndSet(false, true);
    }

    public boolean isTerminated() {
        return terminated.get();
    }

    public byte[] generateInputPbBytes() {
        return request != null ? request.getGenerateInputPbBytes() : null;
    }

    // -- InflightEntry absorbed fields --

    public AtomicBoolean rolledBack() { return rolledBack; }
    public boolean isAckFinished() { return ackFinished; }
    public void setAckFinished(boolean ackFinished) { this.ackFinished = ackFinished; }
    public long batchId() { return batchId; }
    public void setBatchId(long batchId) { this.batchId = batchId; }
    public void setBatchIdIfUnset(long batchId) {
        if (this.batchId < 0) {
            this.batchId = batchId;
        }
    }

    // -- Idempotent cleanup methods (for Q2 CleanupCoordinator) --

    /** Rollback decode KV reservation. CAS-guarded — only executes once. */
    public void rollbackDecode() {
        if (rolledBack.compareAndSet(false, true)) {
            if (decodeEp != null && decode != null) {
                decodeEp.release(requestId());
            }
        }
    }

    /** Repack prefill batch to remove this request. Self-guarding: no-op if batchId < 0. */
    public void repackPrefillBatch() {
        if (batchId < 0) return;
        if (prefillEp != null) {
            prefillEp.repackBatch(batchId, Set.of(requestId()));
        }
    }

    // -- derived accessors --

    /** Total sequence length of this request. */
    public long seqLen() {
        return request != null && request.getRequest() != null
                ? request.getRequest().getSeqLen() : 0;
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

    // -- Object --

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof BatchItem that)) return false;
        return sortKey == that.sortKey && enqueuedAtMs == that.enqueuedAtMs
                && Objects.equals(request, that.request)
                && Objects.equals(routeResponse, that.routeResponse)
                && Objects.equals(prefill, that.prefill)
                && Objects.equals(decode, that.decode)
                && Objects.equals(prefillEp, that.prefillEp)
                && Objects.equals(decodeEp, that.decodeEp);
    }

    @Override
    public int hashCode() {
        return Objects.hash(request, routeResponse, prefill, decode,
                prefillEp, decodeEp, sortKey, enqueuedAtMs);
    }

    @Override
    public String toString() {
        return "BatchItem{requestId=" + requestId() + ", seqLen=" + seqLen()
                + ", sortKey=" + sortKey + ", enqueuedAtMs=" + enqueuedAtMs + '}';
    }
}
