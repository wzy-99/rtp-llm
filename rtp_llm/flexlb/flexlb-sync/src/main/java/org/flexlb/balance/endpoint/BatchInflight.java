package org.flexlb.balance.endpoint;

import org.flexlb.balance.scheduler.BatchItem;
import org.flexlb.balance.scheduler.InflightEvictor;

import java.util.List;

public record BatchInflight(
        long batchId,
        long predictTimeMs,
        List<BatchItem> requests,
        long createdAtMs
) implements InflightEvictor.TtlTracked {
    public BatchInflight(long batchId, long predictTimeMs, List<BatchItem> requests) {
        this(batchId, predictTimeMs, requests, System.currentTimeMillis());
    }
}
