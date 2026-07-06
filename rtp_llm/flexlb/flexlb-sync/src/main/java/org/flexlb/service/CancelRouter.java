package org.flexlb.service;

import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import org.flexlb.balance.scheduler.AbstractScheduler;
import org.springframework.stereotype.Component;

/**
 * Unified cancel entry point for all schedulers.
 *
 * <p>Iterates schedulers in descending priority order and delegates cancellation
 * to the first scheduler that finds the request in-flight.
 *
 * <ol>
 *   <li>BatchScheduler — delegates to {@code BatchScheduler.cancel()}</li>
 *   <li>QueueScheduler — delegates to {@code QueueScheduler.cancel()}</li>
 *   <li>DirectScheduler — no inflight state, returns {@code false}</li>
 * </ol>
 */
@Component
public class CancelRouter {

    private final List<AbstractScheduler> schedulers;

    public CancelRouter(List<AbstractScheduler> schedulers) {
        this.schedulers = schedulers.stream()
                .sorted(Comparator.comparingInt(AbstractScheduler::getOrder).reversed())
                .collect(Collectors.toList());
    }

    /**
     * Cancel a request by ID across all schedulers.
     *
     * @param requestId the request ID to cancel
     * @return true if the request was found and cancelled in any scheduler
     */
    public boolean cancel(long requestId) {
        for (AbstractScheduler scheduler : schedulers) {
            if (scheduler.cancel(requestId)) {
                return true;
            }
        }
        return false;
    }
}
