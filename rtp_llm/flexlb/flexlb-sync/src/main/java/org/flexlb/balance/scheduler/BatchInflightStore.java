package org.flexlb.balance.scheduler;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;

/**
 * Thread-safe store for inflight batch items, keyed by request ID.
 *
 * <p>Replaces the raw {@code ConcurrentHashMap<Long, BatchItem>} that was
 * previously embedded in the former {@code FlexlbBatchScheduler}. Centralising the map
 * allows {@link CleanupCoordinator} and other components to operate on a
 * shared, well-defined abstraction.
 */
@Component
public class BatchInflightStore {

    private final Map<Long, BatchItem> inflight = new ConcurrentHashMap<>();

    public void put(long requestId, BatchItem item) {
        inflight.put(requestId, item);
    }

    public BatchItem remove(long requestId) {
        return inflight.remove(requestId);
    }

    public BatchItem get(long requestId) {
        return inflight.get(requestId);
    }

    public int size() {
        return inflight.size();
    }

    public boolean isEmpty() {
        return inflight.isEmpty();
    }

    /**
     * Iterate over items whose TTL has expired and remove them, invoking the
     * supplied callback for each evicted item.
     *
     * @param ttlMs   maximum age in milliseconds; items older than this are evicted
     * @param onEvict callback invoked once per evicted item (after removal)
     */
    public void forEachEvictable(long ttlMs, Consumer<BatchItem> onEvict) {
        long now = System.currentTimeMillis();
        var it = inflight.entrySet().iterator();
        while (it.hasNext()) {
            var e = it.next();
            BatchItem item = e.getValue();
            if (now - item.createdAtMs() > ttlMs) {
                it.remove();
                onEvict.accept(item);
            }
        }
    }
}
