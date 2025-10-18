package com.nhom7.spark.stream;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;
import com.nhom7.spark.parsing.TransactionParser;
import com.nhom7.spark.rules.RuleEngine;
import com.nhom7.spark.sinks.AlertSink;
import com.nhom7.spark.sinks.MongoSink;
import com.nhom7.spark.sinks.TransactionSink;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.api.java.Optional;

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

        final RuleEngine engine = this.ruleEngine;
        final AlertSink  sink   = this.sink;

        JavaPairDStream<String, UserState> userStates = events
            .updateStateByKey((newTransactions, existingState) -> {
                UserState st = existingState.orElse(UserState.empty());
                for (Transaction txn : newTransactions) {
                    List<Alert> alerts = engine.evaluateAll(txn, st);
                    for (Alert alert : alerts) {
                        sink.send(alert);
                    }
                    
                    st.updateUserState(txn, st);
                }
                return Optional.of(st);
            });
        
        userStates.print();

        // Sink transactions to MongoDB
        JavaDStream<Transaction> txStream = stream.map(ConsumerRecord::value).map(TransactionParser::fromJson);
        final String uri = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017");
        final String db  = System.getenv().getOrDefault("MONGO_DB", "frauddb");
        final String col = System.getenv().getOrDefault("MONGO_COLL_TX", "transactions");

        TransactionSink txSink = new MongoSink(uri, db, col);
        txStream.foreachRDD(txSink::write);

        jssc.start();
        jssc.awaitTermination();
    }
}