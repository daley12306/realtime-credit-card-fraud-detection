package com.nhom7.spark.models;

import java.io.Serializable;
import java.time.Instant;

public final class Transaction implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String transaction_id;
    private final Instant timestamp;
    private final String user_id;
    private final String user_home_region;
    private final String user_home_province;
    private final String card_number;
    private final String card_bank;
    private final String merchant;
    private final String merchant_country;
    private final String merchant_region;
    private final String merchant_province;
    private final String merchant_category;
    private final String transaction_type;
    private final double amount;
    private final String currency;
    private final String device_type;
    private final String device_ip;
    private final String geo_location;
    private final int loyalty_points;
    private final String payment_method;
    private final String transaction_status;

    public Transaction(String transaction_id, Instant timestamp, String user_id, String user_home_region,
                       String user_home_province, String card_number, String card_bank, String merchant,
                       String merchant_country, String merchant_region, String merchant_province,  String merchant_category,
                       String transaction_type, double amount, String currency,
                       String device_type, String device_ip, String geo_location,
                       int loyalty_points, String payment_method, String transaction_status) {
        this.transaction_id = transaction_id;
        this.timestamp = timestamp;
        this.user_id = user_id;
        this.user_home_region = user_home_region;
        this.user_home_province = user_home_province;
        this.card_number = card_number;
        this.card_bank = card_bank;
        this.merchant = merchant;
        this.merchant_country = merchant_country;
        this.merchant_region = merchant_region;
        this.merchant_province = merchant_province;
        this.merchant_category = merchant_category;
        this.transaction_type = transaction_type;
        this.amount = amount;
        this.currency = currency;
        this.device_type = device_type;
        this.device_ip = device_ip;
        this.geo_location = geo_location;
        this.loyalty_points = loyalty_points;
        this.payment_method = payment_method;
        this.transaction_status = transaction_status;
    }

    public String getTransactionId() { return transaction_id; }
    public Instant getTimestamp() { return timestamp; }
    public String getUserId() { return user_id; }
    public String getUserHomeRegion() { return user_home_region; }
    public String getUserHomeProvince() { return user_home_province; }
    public String getCardNumber() { return card_number; }
    public String getCardBank() { return card_bank; }
    public String getMerchant() { return merchant; }
    public String getMerchantCountry() { return merchant_country; }
    public String getMerchantRegion() { return merchant_region; }
    public String getMerchantProvince() { return merchant_province; }
    public String getMerchantCategory() { return merchant_category; }
    public String getTransactionType() { return transaction_type; }
    public double getAmount() { return amount; }
    public String getCurrency() { return currency; }
    public String getDeviceType() { return device_type; }
    public String getDeviceIp() { return device_ip; }
    public String getGeoLocation() { return geo_location; }
    public int getLoyaltyPoints() { return loyalty_points; }
    public String getPaymentMethod() { return payment_method; }
    public String getTransactionStatus() { return transaction_status; }

    @Override public String toString() {
        return "Txn{id=" + transaction_id + ", user=" + user_id + ", amount=" + amount + " " + currency + " at " + timestamp + "}";
    }
}
