package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;

/**
 * Rule phát hiện người dùng đổi loại thiết bị giao dịch (web ↔ pos ↔ mobile...),
 */
public final class DeviceChangeRule implements Rule {
    private static final long serialVersionUID = 1L;

    private final boolean alertOnFirstUse;

    public DeviceChangeRule(boolean alertOnFirstUse) {
        this.alertOnFirstUse = alertOnFirstUse;
    }

    @Override
    public String name() {
        return "DEVICE_CHANGE";
    }

    @Override
    public Optional<Alert> evaluate(Transaction txn, UserState state) {
        Objects.requireNonNull(txn, "txn");
        Objects.requireNonNull(state, "state");

        String currentDeviceType = txn.getDeviceType();
        String lastDeviceType = state.getLastDevice();

        // Không có thông tin thiết bị → bỏ qua
        if (currentDeviceType == null || currentDeviceType.isEmpty()) {
            return Optional.empty();
        }

        // Lần đầu tiên có dữ liệu
        if (lastDeviceType == null || lastDeviceType.isEmpty()) {
            if (alertOnFirstUse) {
                String detail = String.format("first device used: %s", currentDeviceType);
                Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
                return Optional.of(new Alert(name(), txn.getUserId(), detail, ts));
            }
            return Optional.empty();
        }

        // So sánh loại thiết bị (chỉ cảnh báo nếu khác loại)
        if (!currentDeviceType.equalsIgnoreCase(lastDeviceType)) {
            String key = txn.getCardNumber() != null && !txn.getCardNumber().isEmpty()
                    ? txn.getCardNumber()
                    : txn.getUserId();

            String detail = String.format(
                    "device type changed: last=%s, now=%s",
                    lastDeviceType, currentDeviceType
            );

            Instant ts = txn.getTimestamp() != null ? txn.getTimestamp() : Instant.now();
            return Optional.of(new Alert(name(), key, detail, ts));
        }

        // Không cảnh báo nếu cùng loại thiết bị
        return Optional.empty();
    }

    public boolean isAlertOnFirstUse() {
        return alertOnFirstUse;
    }
}
