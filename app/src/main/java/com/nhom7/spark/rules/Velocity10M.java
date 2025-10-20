package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.time.Duration;
import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public final class Velocity10M implements Rule {
    private static final long serialVersionUID = 1L;

    private final int maxTxns;
    private final Duration window;

    public Velocity10M(int maxTxns) {
        this(maxTxns, Duration.ofMinutes(10));
    }

    public Velocity10M(int maxTxns, Duration window) {
        if (maxTxns <= 0) throw new IllegalArgumentException("maxTxns must be > 0");
        Objects.requireNonNull(window, "window");
        this.maxTxns = maxTxns;
        this.window = window;
    }

    @Override
    public String name() { return "VELOCITY_10M"; }

    @Override
    public Optional<Alert> evaluate(Transaction txn, UserState state) {
        Objects.requireNonNull(txn, "txn");
        Objects.requireNonNull(state, "state");

        Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
        Instant since = ts.minus(window);

        // count transactions in state since 'since'
        int countInState = state.countTransactionsSince(since);

        // include current txn as state may be updated after evaluate -> +1
        int total = countInState + 1;

        if (total >= maxTxns) {
            String key = txn.getCardNumber() != null && !txn.getCardNumber().isEmpty()
                    ? txn.getCardNumber() : txn.getUserId();

            String detail = String.format("txns=%d in last %d sec (threshold=%d)",
                    total, window.getSeconds(), maxTxns);

            return Optional.of(new Alert(name(), key, detail, ts));
        }

        return Optional.empty();
    }

    public int getMaxTxns() { return maxTxns; }
    public Duration getWindow() { return window; }
}