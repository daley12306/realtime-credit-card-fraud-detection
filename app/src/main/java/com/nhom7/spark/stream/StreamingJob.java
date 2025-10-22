package com.nhom7.spark.stream;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;
import com.nhom7.spark.parsing.TransactionParser;
import com.nhom7.spark.rules.GeoVelocityRule;
import com.nhom7.spark.rules.HighAmountRule;
import com.nhom7.spark.rules.DeviceChangeRule;
import com.nhom7.spark.rules.RapidDeclinesRule;
import com.nhom7.spark.rules.RuleEngine;
import com.nhom7.spark.rules.Velocity10M;
import com.nhom7.spark.sinks.AlertSink;
import com.nhom7.spark.sinks.ConsoleAlertSink;
import com.nhom7.spark.sinks.MongoTransactionSink;
import com.nhom7.spark.sinks.TransactionSink;

import scala.Tuple2;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka010.*;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.broadcast.Broadcast;

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
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.sql.adaptive.enabled", "true")
                .set("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .set("spark.streaming.stopGracefullyOnShutdown", "true")
                .set("spark.streaming.backpressure.enabled", "true")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                .set("spark.executor.memoryFraction", "0.8");

        conf.registerKryoClasses(new Class[]{
            RuleEngine.class,
            HighAmountRule.class,
            GeoVelocityRule.class,
            DeviceChangeRule.class,
            RapidDeclinesRule.class,
            Velocity10M.class,
            UserState.class,
            Alert.class,
            Transaction.class,
            ConsoleAlertSink.class,
            MongoTransactionSink.class
        });

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

        final Broadcast<RuleEngine> bcEngine = jssc.sparkContext().broadcast(this.ruleEngine);
        final Broadcast<AlertSink>  bcSink   = jssc.sparkContext().broadcast(this.sink);

        StateUpdater updater = new StateUpdater(bcEngine, bcSink);

        JavaPairDStream<String, UserState> userStates = events
            .updateStateByKey(updater);

        userStates.print();

        // Sink transactions to MongoDB
        JavaDStream<Transaction> txStream = stream.map(ConsumerRecord::value).map(TransactionParser::fromJson);
        final String uri = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongo:27017");
        final String db  = System.getenv().getOrDefault("MONGO_DB", "frauddb");
        final String col = System.getenv().getOrDefault("MONGO_COLL_TX", "transactions");

        TransactionSink txSink = new MongoTransactionSink(uri, db, col);
        txStream.foreachRDD(txSink::write);

        jssc.start();
        jssc.awaitTermination();
    }

    /**
     * Static updatable function to avoid capturing StreamingJob.this in closures.
     * Implements the signature expected by updateStateByKey: (List<V>, Optional<S>) -> Optional<S>
     */
    public static final class StateUpdater implements Function2<List<Transaction>, Optional<UserState>, Optional<UserState>> {
        private static final long serialVersionUID = 1L;

        private final Broadcast<RuleEngine> engineBc;
        private final Broadcast<AlertSink> sinkBc;

        public StateUpdater(Broadcast<RuleEngine> engineBc, Broadcast<AlertSink> sinkBc) {
            this.engineBc = engineBc;
            this.sinkBc = sinkBc;
        }

        @Override
        public Optional<UserState> call(List<Transaction> newTransactions, Optional<UserState> existingState) throws Exception {
            UserState st = existingState.orElse(UserState.empty());

            RuleEngine engine = engineBc != null ? engineBc.getValue() : null;
            AlertSink sink = sinkBc != null ? sinkBc.getValue() : null;

            for (Transaction txn : newTransactions) {
                if (engine != null) {
                    List<com.nhom7.spark.models.Alert> alerts = engine.evaluateAll(txn, st);
                    if (sink != null) {
                        for (com.nhom7.spark.models.Alert a : alerts) sink.send(a);
                    }
                }

                // update state with txn
                st.updateUserState(txn, st);
            }

            return Optional.of(st);
        }
    }
}