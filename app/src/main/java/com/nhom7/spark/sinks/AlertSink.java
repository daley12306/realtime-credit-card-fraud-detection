package com.nhom7.spark.sinks;

import java.io.Serializable;

import com.nhom7.spark.models.Alert;

public interface AlertSink extends Serializable{
    void send(Alert alert);
}
