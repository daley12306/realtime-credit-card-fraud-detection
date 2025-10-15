package com.nhom7.spark.models;

import java.time.Instant;
import java.util.Objects;
import java.io.Serializable;

public final class Alert implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String rule;
    private final String key;     // userId hoặc cardNumber
    private final String details;
    private final Instant at;

    public Alert(String rule, String key, String details, Instant at) {
        this.rule = Objects.requireNonNull(rule);
        this.key = Objects.requireNonNull(key);
        this.details = Objects.requireNonNull(details);
        this.at = Objects.requireNonNull(at);
    }

    public String getRule(){ return rule; }
    public String getKey(){ return key; }
    public String getDetails(){ return details; }
    public Instant getAt(){ return at; }

    @Override public String toString(){ return "ALERT[" + rule + "] key=" + key + " details=" + details + " at=" + at; }
}
