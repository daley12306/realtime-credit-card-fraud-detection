package com.nhom7.spark.sinks;

import com.nhom7.spark.models.Transaction;
import org.bson.Document;

public final class TxnDocMapper {
    private TxnDocMapper() {}

    public static Document toDoc(Transaction t) {
        return new Document()
            .append("_id", t.getTransactionId())
            .append("timestamp", t.getTimestamp() != null ? t.getTimestamp().toString() : null)
            .append("user_id", t.getUserId())
            .append("user_home_region", t.getUserHomeRegion())
            .append("user_home_province", t.getUserHomeProvince())
            .append("card_number", t.getCardNumber())
            .append("card_bank", t.getCardBank())
            .append("merchant", t.getMerchant())
            .append("merchant_country", t.getMerchantCountry())
            .append("merchant_region", t.getMerchantRegion())
            .append("merchant_province", t.getMerchantProvince())
            .append("merchant_category", t.getMerchantCategory())
            .append("transaction_type", t.getTransactionType())
            .append("amount", t.getAmount())
            .append("currency", t.getCurrency())
            .append("device_type", t.getDeviceType())
            .append("device_ip", t.getDeviceIp())
            .append("geo_location", t.getGeoLocation())
            .append("loyalty_points", t.getLoyaltyPoints())
            .append("payment_method", t.getPaymentMethod())
            .append("transaction_status", t.getTransactionStatus());

    }
}
