package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.time.Instant;
import java.time.Duration;
import java.util.Objects;
import java.util.Optional;

public final class RapidDeclinesRule implements Rule {
    private static final long serialVersionUID = 1L;

    private final int threshold;   // number of consecutive declined transactions
    private final Duration window; // time window to check


    public RapidDeclinesRule(int threshold, Duration window) {
        if (threshold <= 0)
            throw new IllegalArgumentException("threshold must be > 0");
        this.threshold = threshold;
        this.window = Objects.requireNonNull(window, "window");
    }

    @Override
    public String name() {
        return "RAPID_DECLINES";
    }

    @Override
    public Optional<Alert> evaluate(Transaction txn, UserState state) {
        Objects.requireNonNull(txn, "txn");
        Objects.requireNonNull(state, "state");

        // chỉ xét nếu giao dịch bị từ chối
        if (txn.getTransactionStatus() == null || !txn.getTransactionStatus().equalsIgnoreCase("DECLINED")) {
            return Optional.empty();
        }

        Instant now = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
        Instant since = now.minus(window);

        int recentCount = state.countTransactionsSince(since);

        if (recentCount >= threshold) {
            String key = txn.getCardNumber() != null && !txn.getCardNumber().isEmpty()
                    ? txn.getCardNumber() : txn.getUserId();

            String detail = String.format("%d declined transactions within %d seconds",
                    recentCount, window.getSeconds());

            return Optional.of(new Alert(name(), key, detail, now));
        }

        return Optional.empty();
    }

    public int getThreshold() {
        return threshold;
    }

    public Duration getWindow() {
        return window;
    }

}
