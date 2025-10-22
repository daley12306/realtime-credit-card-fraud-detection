package com.nhom7.spark.app;

import com.nhom7.spark.rules.*;
import com.nhom7.spark.sinks.AlertSink;
import com.nhom7.spark.sinks.ConsoleAlertSink;
import com.nhom7.spark.sinks.MongoAlertSink;
import com.nhom7.spark.stream.StreamingJob;

import java.time.Duration;
import java.util.Arrays;

public final class Application {
    public static void main(String[] args) throws Exception {
        String master = env("SPARK_MASTER", "spark://spark-master:7077");
        String host   = env("KAFKA_HOST", "kafka");
        int    port   = Integer.parseInt(env("KAFKA_PORT", "9092"));
        String topic  = env("KAFKA_TOPIC", "txs");
        int    batch  = Integer.parseInt(env("BATCH_INTERVAL", "1"));

        RuleEngine engine = new RuleEngine(Arrays.asList(
                new HighAmountRule(10.0),
                new Velocity10M(5),
                new DeviceChangeRule(true),
                new GeoVelocityRule(700),
                new RapidDeclinesRule(2, Duration.ofSeconds(30))
        ));
        
        // AlertSink alertSink = new ConsoleAlertSink();

        final String mongoUri  = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017");
        final String mongoDb   = System.getenv().getOrDefault("MONGO_DB", "frauddb");
        final String mongoCollAlerts = System.getenv().getOrDefault("MONGO_COLL_ALERTS", "alerts");

        AlertSink alertSink = new MongoAlertSink(mongoUri, mongoDb, mongoCollAlerts);

        new StreamingJob(master, host, port, topic, batch, engine, alertSink).start();
    }

    private static String env(String k, String d){
        String v = System.getenv(k);
        return (v == null || v.isEmpty()) ? d : v;
    }
}
