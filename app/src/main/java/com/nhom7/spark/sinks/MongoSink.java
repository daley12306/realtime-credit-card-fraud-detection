package com.nhom7.spark.sinks;

import com.mongodb.client.*;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.nhom7.spark.models.Transaction;
import org.apache.spark.api.java.JavaRDD;
import org.bson.Document;
import java.util.*;

public final class MongoSink implements TransactionSink {
    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String db;
    private final String collection;
    private final int bulkSize;

    public MongoSink(String uri, String db, String collection) {
        this(uri, db, collection, 1000);
    }

    public MongoSink(String uri, String db, String collection, int bulkSize) {
        this.uri = Objects.requireNonNull(uri);
        this.db = Objects.requireNonNull(db);
        this.collection = Objects.requireNonNull(collection);
        this.bulkSize = bulkSize > 0 ? bulkSize : 1000;
    }

    @Override
    public void write(JavaRDD<Transaction> rdd) {
        rdd.foreachPartition(part -> {
            if (!part.hasNext()) return;

            try (MongoClient client = MongoClients.create(uri)) {
                MongoCollection<Document> coll = client.getDatabase(db).getCollection(collection);
                ReplaceOptions upsert = new ReplaceOptions().upsert(true);

                List<WriteModel<Document>> ops = new ArrayList<>(bulkSize);
                while (part.hasNext()) {
                    Transaction t = part.next();
                    Document doc = TxnDocMapper.toDoc(t);
                    ops.add(new ReplaceOneModel<>(new Document("_id", doc.get("_id")), doc, upsert));

                    if (ops.size() >= bulkSize) {
                        coll.bulkWrite(ops);
                        ops.clear();
                    }
                }
                if (!ops.isEmpty()) coll.bulkWrite(ops);
            }
        });
    }
}
