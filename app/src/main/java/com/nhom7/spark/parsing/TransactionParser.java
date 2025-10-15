package com.nhom7.spark.parsing;

import com.nhom7.spark.models.Transaction;
import org.json.JSONObject;
import java.time.Instant;

public final class TransactionParser {
    private TransactionParser() {}

    public static Transaction fromJson(String json) {
        JSONObject o = new JSONObject(json);
        return new Transaction(
            o.getString("transaction_id"),
            Instant.parse(o.getString("timestamp")),
            o.getString("user_id"),
            o.getString("user_home_region"),
            o.getString("user_home_province"),
            o.getString("card_number"),
            o.getString("card_bank"),
            o.getString("merchant"),
            o.getString("merchant_country"),
            o.getString("merchant_region"),
            o.getString("merchant_province"),
            o.getString("merchant_category"),
            o.getString("transaction_type"),
            o.getDouble("amount"),
            o.getString("currency"),
            o.getString("device_type"),
            o.getString("device_ip"),
            o.getString("geo_location"),
            o.getInt("loyalty_points"),
            o.getString("payment_method"),
            o.getString("transaction_status")
        );
    }
}
