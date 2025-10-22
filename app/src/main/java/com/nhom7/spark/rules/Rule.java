package com.nhom7.spark.rules;

import com.nhom7.spark.models.Alert;
import com.nhom7.spark.models.Transaction;

import java.util.List;
import java.util.Optional;
import java.io.Serializable;
import com.nhom7.spark.models.UserState;

public interface Rule extends Serializable {
    String name();
    Optional<Alert> evaluate(Transaction txn, UserState state);
}
