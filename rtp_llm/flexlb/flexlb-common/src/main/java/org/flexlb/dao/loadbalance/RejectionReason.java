package org.flexlb.dao.loadbalance;

/**
 * Fine-grained reason a candidate endpoint was rejected during the filter stage
 * of a {@link LoadBalanceStrategy#select} call.
 *
 * <p>Used as the key in the {@code rejections} map on {@link ServerStatus} when
 * selection fails. Consumed by {@code DefaultRouter.handleSelectFailure} for
 * key-based eviction routing with multi-handler fallback.
 *
 * <ul>
 *   <li>{@link #RESOURCE_UNAVAILABLE} &rarr; Stage 2 prefill batcher PQ eviction</li>
 *   <li>{@link #KV_CAPACITY}, {@link #KV_UNAVAILABLE} &rarr; Stage 3a decode KV token eviction</li>
 *   <li>{@link #COMPUTE_SATURATED} &rarr; Stage 3b decode concurrency eviction</li>
 *   <li>Others &rarr; logged but no eviction triggered</li>
 * </ul>
 */
public enum RejectionReason {
    // Common — produced by both Decode and Prefill strategies
    /** ResourceMeasureFactory has no registered measure for the indicator. */
    NO_REGISTERED,
    /** Endpoint is not alive (health check failed or not yet reported). */
    NOT_ALIVE,
    /** Endpoint load/wait exceeds the hotspot threshold relative to the average. */
    HOTSPOT_FILTERED,
    /** Endpoint KV usage or pending count exceeds the imbalance threshold. */
    IMBALANCE_FILTERED,

    // Decode-specific (CostBasedDecodeStrategy)
    /** Decode concurrency saturated: totalLoad >= decodeConcurrencyLimit. */
    COMPUTE_SATURATED,
    /** Stage 1: resource unavailable but not concurrency-saturated. */
    KV_UNAVAILABLE,
    /** Stage 2: available KV cache < required sequence length. */
    KV_CAPACITY,

    // Prefill-specific (CostBasedPrefillStrategy)
    /** Stage 1: measure reports resource unavailable for prefill. */
    RESOURCE_UNAVAILABLE,
    /** Stage 2: endpoint has no cost predictor configured. */
    PREDICTOR_MISSING,
    /** Stage 2: predicted wait + prefill time exceeds SLO budget. */
    SLO_VIOLATION
}
