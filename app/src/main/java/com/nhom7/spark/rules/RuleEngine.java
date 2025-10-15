package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;
import com.nhom7.spark.models.UserState;

import java.io.Serializable;
import java.util.*;

public final class RuleEngine implements Serializable {
    private static final long serialVersionUID = 1L;
    
    private final List<Rule> rules;

    public RuleEngine(List<Rule> rules) {
        this.rules = Collections.unmodifiableList(new ArrayList<>(Objects.requireNonNull(rules)));
    }

    public List<Alert> evaluateAll(Transaction txn, UserState state) {
        List<Alert> results = new ArrayList<>();
        for (Rule rule : rules) {
            rule.evaluate(txn, state).ifPresent(results::add);
        }
        return Collections.unmodifiableList(results);
    }

    public List<Alert> evaluateAll(Transaction txn) {
        return evaluateAll(txn, UserState.empty());
    }

    public List<Rule> getRules() {
        return rules;
    }
}
