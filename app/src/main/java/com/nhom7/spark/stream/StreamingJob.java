package com.nhom7.spark.stream;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;
import com.nhom7.spark.parsing.TransactionParser;
import com.nhom7.spark.rules.RuleEngine;
import com.nhom7.spark.sinks.AlertSink;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.State;
import org.apache.spark.streaming.StateSpec;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;

import java.io.Serializable;
import java.util.*;

public final class StreamingJob implements Serializable{
    private final String masterUrl;
    private final String topic;
    private final int batchSec;
    private final Map<String,Object> kafkaParams;
    private final RuleEngine ruleEngine;
    private final AlertSink sink;

    public StreamingJob(String masterUrl,
                        String brokersHost, int brokersPort,
                        String topic, int batchSec,
                        RuleEngine ruleEngine,
                        AlertSink sink) {
        this.masterUrl   = Objects.requireNonNull(masterUrl);
        this.topic       = Objects.requireNonNull(topic);
        this.batchSec    = batchSec;
        this.kafkaParams = KafkaConfig.build(brokersHost, brokersPort);
        this.ruleEngine  = Objects.requireNonNull(ruleEngine);
        this.sink        = Objects.requireNonNull(sink);
    }

    public void start() throws Exception {
        SparkConf conf = new SparkConf()
                .setAppName("Credit Card Fraud Detection")
                .setMaster(masterUrl)
                .set("spark.executor.memory", "1g");

        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(batchSec * 1000));
        jssc.sparkContext().setLogLevel("ERROR");
        jssc.checkpoint("/tmp/checkpoints");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jssc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String,String>Subscribe(Collections.singletonList(topic), kafkaParams)
                );

        JavaPairDStream<String, Transaction> events = stream
        .map(ConsumerRecord::value)
        .mapToPair(json -> {
            Transaction txn = TransactionParser.fromJson(json);
            return new Tuple2<>(txn.getUserId(), txn);
        });

        final RuleEngine engine = this.ruleEngine; // implements Serializable
        final AlertSink  sink   = this.sink;

        org.apache.spark.api.java.function.Function3<
        String,                      // key = userId
        org.apache.spark.api.java.Optional<Transaction>,  // event ở batch này (có thể trống khi timeout)
        State<UserState>,            // state hiện tại
        List<Alert>                  // KẾT QUẢ phát ra cho stream output
    > mappingFunction = (userId, maybeTxn, state) -> {

        UserState st = state.exists() ? state.get() : UserState.empty();

        if (!maybeTxn.isPresent()) {
            if (state.isTimingOut()) {
            }
            return java.util.Collections.emptyList();
        }

        Transaction txn = maybeTxn.get();

        List<Alert> alerts = engine.evaluateAll(txn, st);
        st.updateUserState(txn, st);
        state.update(st);
        return alerts;
    };

    StateSpec<String, Transaction, UserState, List<Alert>> spec =
        StateSpec.function(mappingFunction)
                 .timeout(Durations.minutes(30)); // tuỳ: TTL per user

    JavaMapWithStateDStream<String, Transaction, UserState, List<Alert>> alertStream =
        events.mapWithState(spec);

    alertStream.foreachRDD(rdd -> {
        rdd.flatMap(list -> list.iterator())
           .foreach(a -> sink.send(a));
    });

        jssc.start();
        jssc.awaitTermination();
    }
}
