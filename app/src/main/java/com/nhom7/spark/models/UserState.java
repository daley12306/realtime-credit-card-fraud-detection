package com.nhom7.spark.models;

import java.io.Serializable;
import java.time.Instant;
import java.time.Duration;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Objects;

public class UserState implements Serializable {
    public double lastLat = 0;
    public double lastLon = 0;
    public Instant lastTime = null;
    public String lastDevice = null;
    public double lastAverageSpend = 0;

    private final Deque<Instant> recentTimestamps = new ArrayDeque<>();

    private static final Duration TIMESTAMP_RETENTION = Duration.ofMinutes(15);

    public static UserState empty() { return new UserState(); }

    public void updateUserState(Transaction txn, UserState state) {
        if (txn.getGeoLocation() != null && !txn.getGeoLocation().isEmpty()) {
            String[] parts = txn.getGeoLocation().split(",");
            if (parts.length == 2) {
                try {
                    state.lastLat = Double.parseDouble(parts[0]);
                    state.lastLon = Double.parseDouble(parts[1]);
                } catch (NumberFormatException e) {
                    // Ignore invalid format
                }
            }
        }
        state.lastTime = txn.getTimestamp();
        state.lastDevice = txn.getDeviceType();
        // Update average spend using a simple moving average formula
        state.lastAverageSpend = (state.lastAverageSpend + txn.getAmount()) / 2;
    
        // record timestamp for velocity checks
        Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
        state.recordTransactionTimestamp(ts);
    }

    public double getAverageAmount() { return lastAverageSpend; }
    public double getLastLat() { return lastLat; }
    public double getLastLon() { return lastLon; }
    public Instant getLastTime() { return lastTime; }
    public String getLastDevice() { return lastDevice; }

    /**
     * Record a transaction timestamp into the sliding-window deque.
     * Synchronized to be safe for concurrent access.
     */
    public synchronized void recordTransactionTimestamp(Instant ts) {
        Objects.requireNonNull(ts, "ts");
        recentTimestamps.addLast(ts);

        Instant cutoff = ts.minus(TIMESTAMP_RETENTION);
        while (!recentTimestamps.isEmpty() && recentTimestamps.peekFirst().isBefore(cutoff)) {
            recentTimestamps.pollFirst();
        }
    }

    /**
     * Count transactions since 'since' (inclusive). Also prune older entries.
     * Returns number of timestamps >= since.
     */
    public synchronized int countTransactionsSince(Instant since) {
        Objects.requireNonNull(since, "since");
        // prune older timestamps at head
        while (!recentTimestamps.isEmpty() && recentTimestamps.peekFirst().isBefore(since)) {
            recentTimestamps.pollFirst();
        }
        return recentTimestamps.size();
    }
}