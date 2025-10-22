package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.Locale;

public final class GeoVelocityRule implements Rule {
    private static final long serialVersionUID = 1L;

    // threshold km/h above which we consider movement impossible
    private final double maxSpeedKmh;

    // giảm mặc định xuống giá trị hợp lý hơn
    public GeoVelocityRule() { this(700.0); }

    public GeoVelocityRule(double maxSpeedKmh) {
        if (maxSpeedKmh <= 0) throw new IllegalArgumentException("maxSpeedKmh must be > 0");
        this.maxSpeedKmh = maxSpeedKmh;
    }

    @Override public String name() { return "GEO_VELOCITY"; }

    @Override
    public Optional<Alert> evaluate(Transaction txn, UserState state) {
        Objects.requireNonNull(txn, "txn");
        Objects.requireNonNull(state, "state");

        String geo = txn.getGeoLocation();
        if (geo == null || geo.trim().isEmpty()) return Optional.empty();
        Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();

        // Need previous location and time
        Instant prevTime = state.getLastTime();
        double prevLat = state.getLastLat();
        double prevLon = state.getLastLon();
        if (prevTime == null) return Optional.empty();
        // if previous coords are both 0, assume missing
        if (prevLat == 0.0 && prevLon == 0.0) return Optional.empty();

        // accept formats "lat,lon" or "lat;lon"
        String[] parts = geo.split("[,;]");
        if (parts.length != 2) return Optional.empty();
        double lat, lon;
        try {
            lat = Double.parseDouble(parts[0].trim());
            lon = Double.parseDouble(parts[1].trim());
        } catch (NumberFormatException e) {
            return Optional.empty();
        }

        double deltaSec = Math.abs(Duration.between(prevTime, ts).toMillis() / 1000.0);
        if (deltaSec < 1e-6) return Optional.empty(); // avoid division by zero / spurious

        double distKm = haversine(prevLat, prevLon, lat, lon);
        double speedKmh = distKm * 3600.0 / deltaSec;

        if (speedKmh > maxSpeedKmh) {
            String key = txn.getCardNumber() != null && !txn.getCardNumber().isEmpty()
                    ? txn.getCardNumber() : txn.getUserId();

            String detail = String.format(Locale.ROOT, "dist=%.2f km, dt=%.1f sec, speed=%.1f km/h (threshold=%.1f)",
                    distKm, deltaSec, speedKmh, maxSpeedKmh);

            return Optional.of(new Alert(name(), key, detail, ts));
        }

        return Optional.empty();
    }

    public double getMaxSpeedKmh() { return maxSpeedKmh; }

    // Haversine formula to compute distance between two lat/lon points in kilometers
    private static double haversine(double lat1, double lon1, double lat2, double lon2) {
        final double R = 6371.0; // Earth radius km
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2)
                + Math.cos(Math.toRadians(lat1)) * Math.cos(Math.toRadians(lat2))
                * Math.sin(dLon / 2) * Math.sin(dLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        return R * c;
    }
}