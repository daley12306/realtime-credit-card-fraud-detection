package com.nhom7.spark.app;

import com.nhom7.spark.rules.HighAmountRule;
import com.nhom7.spark.rules.RuleEngine;
import com.nhom7.spark.sinks.AlertSink;
import com.nhom7.spark.sinks.ConsoleAlertSink;
import com.nhom7.spark.stream.StreamingJob;
import com.nhom7.spark.rules.DeviceChangeRule;

import java.util.Arrays;

public final class Application {
    public static void main(String[] args) throws Exception {
        String master = env("SPARK_MASTER", "spark://spark-master:7077");
        String host   = env("KAFKA_HOST", "kafka");
        int    port   = Integer.parseInt(env("KAFKA_PORT", "9092"));
        String topic  = env("KAFKA_TOPIC", "txs");
        int    batch  = Integer.parseInt(env("BATCH_INTERVAL", "1"));

        RuleEngine engine = new RuleEngine(Arrays.asList(
                new HighAmountRule(20.0),
                // thêm rule khác ở đây (DeviceChangeRule, GeoVelocityRule, v.v.)
                new DeviceChangeRule(true)
        ));
        AlertSink sink = new ConsoleAlertSink();

        new StreamingJob(master, host, port, topic, batch, engine, sink).start();
    }

    private static String env(String k, String d){
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? d : v;
    }
}
