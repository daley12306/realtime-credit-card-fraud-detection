package com.nhom7.spark.sinks;

import com.nhom7.spark.models.Transaction;
import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface TransactionSink extends Serializable {
    void write(JavaRDD<Transaction> rdd);
}
