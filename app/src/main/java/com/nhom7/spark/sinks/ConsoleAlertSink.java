package com.nhom7.spark.sinks;


import com.nhom7.spark.models.Alert;

public final class ConsoleAlertSink implements AlertSink {

    @Override public void send(Alert alert) {
        System.out.println(alert);
    }
}