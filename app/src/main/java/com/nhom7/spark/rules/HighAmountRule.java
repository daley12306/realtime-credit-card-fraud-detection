package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

public final class HighAmountRule implements Rule {
    private static final long serialVersionUID = 1L;

    private final double multiplier;  // ví dụ: 2.5 nghĩa là >2.5x trung bình thì cảnh báo

    public HighAmountRule(double multiplier) {
        if (multiplier <= 1.0)
            throw new IllegalArgumentException("multiplier must be > 1.0");
        this.multiplier = multiplier;
    }

    @Override
    public String name() { return "HIGH_AMOUNT"; }

    @Override
    public Optional<Alert> evaluate(Transaction txn, UserState state) {
        Objects.requireNonNull(txn, "txn");
        Objects.requireNonNull(state, "state");

        double avg = state.getAverageAmount();
        double amount = txn.getAmount();

        if (avg > 0 && amount > avg * multiplier) {
            String key = txn.getCardNumber() != null && !txn.getCardNumber().isEmpty()
                    ? txn.getCardNumber() : txn.getUserId();

            String detail = String.format(
                    "amount=%.2f > %.1fx avg (avg=%.2f)",
                    amount, multiplier, avg
            );

            Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
            return Optional.of(new Alert(name(), key, detail, ts));
        }

        // Không alert nếu chưa có dữ liệu trung bình
        return Optional.empty();
    }

    public double getMultiplier() { return multiplier; }
}
