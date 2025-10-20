package com.nhom7.spark.sinks;

import com.nhom7.spark.models.Alert;
import com.mongodb.client.*;
import org.bson.Document;
import com.mongodb.client.model.ReplaceOptions;

import java.io.Serializable;
import java.util.Objects;

public final class MongoAlertSink implements AlertSink, Serializable {
    private static final long serialVersionUID = 1L;

    private final String uri;
    private final String db;
    private final String col;

    public MongoAlertSink(String uri, String db, String col) {
        this.uri = Objects.requireNonNull(uri);
        this.db = Objects.requireNonNull(db);
        this.col = Objects.requireNonNull(col);
    }

    @Override
    public void send(Alert alert) {
        try (MongoClient client = MongoClients.create(uri)) {
            MongoCollection<Document> collection =
                    client.getDatabase(db).getCollection(col);

            Document doc = new Document("_id", alert.getKey() + "_" + alert.getAt().toEpochMilli())
                    .append("rule", alert.getRule())
                    .append("key", alert.getKey())
                    .append("details", alert.getDetails())
                    .append("timestamp", alert.getAt().toString());

            // Upsert tránh ghi trùng
            collection.replaceOne(
                    new Document("_id", doc.get("_id")),
                    doc,
                    new ReplaceOptions().upsert(true)
            );
        } catch (Exception e) {
            System.err.println("MongoAlertSink error: " + e.getMessage());
        }
    }
}
