package org.flexlb.balance.strategy;

import org.flexlb.balance.scheduler.BatchItem;

import java.util.List;

public record BatcherSnapshot(
        int queueSize,
        List<BatchItem> requests,
        long earliestEnqueueTimeMs,
        long headDeadlineMs) {

    public static final BatcherSnapshot EMPTY = new BatcherSnapshot(0, List.of(), Long.MAX_VALUE, Long.MAX_VALUE);

    public long totalInputTokens() {
        long total = 0;
        for (BatchItem r : requests) {
            total += r.seqLen();
        }
        return total;
    }

    public long totalHitCacheTokens() {
        long total = 0;
        for (BatchItem r : requests) {
            total += r.hitCache();
        }
        return total;
    }
}
