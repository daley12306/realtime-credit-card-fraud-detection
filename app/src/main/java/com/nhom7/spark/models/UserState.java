package com.nhom7.spark.models;

import java.io.Serializable;
import java.time.Instant;

public class UserState implements Serializable {
    public double lastLat = 0;
    public double lastLon = 0;
    public Instant lastTime = null;
    public String lastDevice = null;
    public double lastAverageSpend = 0;

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
    }

    public double getAverageAmount() { return lastAverageSpend; }
    public double getLastLat() { return lastLat; }
    public double getLastLon() { return lastLon; }
    public Instant getLastTime() { return lastTime; }
    public String getLastDevice() { return lastDevice; }
}