package org.flexlb.balance.cost;

import org.flexlb.dao.loadbalance.RejectionReason;

/**
 * Default implementation of {@link EvictionCostFunction} with configurable
 * constants.
 *
 * <p>Default behavior:
 * <ul>
 *   <li>{@code f(priority) = B^rank(priority)} — exponential priority weight where
 *       {@code B = maxVictimsPerDecision + 1} (default B=9, i.e. maxVictimsPerDecision=8).
 *       Priority levels [30,40,50,60,70] map to ranks 0..4 so that
 *       f(30)=1, f(40)=9, f(50)=81, f(60)=729, f(70)=6561.</li>
 *   <li>{@code g(0) = gNotAccepted} (1.0), {@code g(1) = gAcceptedNotRunning} (4.0),
 *       {@code g(2) = gRunning} (16.0) — powers of 4</li>
 *   <li>{@code j(kvTokens) = max(1, ceil(kvTokens / m))} — ceiling bucket</li>
 *   <li>{@code k(kvTokens) = 1.0 / j(kvTokens)} — inverse of j</li>
 *   <li>{@code h(KV_CAPACITY) = h(KV_UNAVAILABLE) = hKv} (1.0),
 *       {@code h(COMPUTE_SATURATED) = h(RESOURCE_UNAVAILABLE) = hSlot} (1.0),
 *       others = 0.0</li>
 * </ul>
 */
public class DefaultEvictionCostFunction implements EvictionCostFunction {

    private static final int[] DEFAULT_PRIORITY_LEVELS = {30, 40, 50, 60, 70};

    private final long m;
    private final double gNotAccepted;
    private final double gAcceptedNotRunning;
    private final double gRunning;
    private final double hSlot;
    private final double hKv;
    private final int maxVictimsPerDecision;
    private final int[] priorityLevels;

    /** Convenience constructor with all defaults. */
    public DefaultEvictionCostFunction() {
        this(1024L, 1.0, 4.0, 16.0, 1.0, 1.0, 8, DEFAULT_PRIORITY_LEVELS);
    }

    /**
     * Full constructor allowing external configuration (e.g. from {@link org.flexlb.config.FlexlbConfig}).
     *
     * @param m                      KV token normalisation divisor (default 1024)
     * @param gNotAccepted           weight for NOT_ACCEPTED run status (ordinal 0)
     * @param gAcceptedNotRunning    weight for ACCEPTED_NOT_RUNNING run status (ordinal 1)
     * @param gRunning               weight for RUNNING run status (ordinal 2)
     * @param hSlot                  scenario weight for slot/concurrency-related cases
     * @param hKv                    scenario weight for KV-related cases
     * @param maxVictimsPerDecision  max victims per eviction decision; B = this + 1
     * @param priorityLevels         ordered priority levels for rank computation
     */
    public DefaultEvictionCostFunction(
            long m,
            double gNotAccepted,
            double gAcceptedNotRunning,
            double gRunning,
            double hSlot,
            double hKv,
            int maxVictimsPerDecision,
            int[] priorityLevels
    ) {
        this.m = m;
        this.gNotAccepted = gNotAccepted;
        this.gAcceptedNotRunning = gAcceptedNotRunning;
        this.gRunning = gRunning;
        this.hSlot = hSlot;
        this.hKv = hKv;
        this.maxVictimsPerDecision = maxVictimsPerDecision;
        this.priorityLevels = priorityLevels != null && priorityLevels.length > 0
                ? priorityLevels : DEFAULT_PRIORITY_LEVELS;
    }

    /**
     * Computes the rank of the given priority within the configured priority levels.
     * If the priority is not an exact match, the nearest lower level's rank is used.
     * Unknown priorities (below all levels) default to rank 0.
     */
    private int rank(int priority) {
        int rank = 0;
        for (int i = 0; i < priorityLevels.length; i++) {
            if (priorityLevels[i] <= priority) {
                rank = i;
            } else {
                break;
            }
        }
        return rank;
    }

    @Override
    public double f(int priority) {
        int b = maxVictimsPerDecision + 1;
        return Math.pow(b, rank(priority));
    }

    @Override
    public double g(int runStatusOrdinal) {
        return switch (runStatusOrdinal) {
            case 0 -> gNotAccepted;
            case 1 -> gAcceptedNotRunning;
            case 2 -> gRunning;
            default -> gRunning; // unknown ordinals default to most expensive
        };
    }

    @Override
    public double j(long kvTokens) {
        return Math.max(1L, (long) Math.ceil((double) kvTokens / m));
    }

    @Override
    public double k(long kvTokens) {
        return 1.0 / j(kvTokens);
    }

    @Override
    public double h(RejectionReason caseType) {
        if (caseType == null) {
            return 0.0;
        }
        return switch (caseType) {
            case KV_CAPACITY, KV_UNAVAILABLE -> hKv;
            case COMPUTE_SATURATED, RESOURCE_UNAVAILABLE -> hSlot;
            default -> 0.0;
        };
    }
}
