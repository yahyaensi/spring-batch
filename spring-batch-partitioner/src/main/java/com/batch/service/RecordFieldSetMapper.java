package com.batch.service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

import com.batch.model.Transaction;

public class RecordFieldSetMapper implements FieldSetMapper<Transaction> {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("d/M/yyy");

    @Override
    public Transaction mapFieldSet(FieldSet fieldSet) {

        Transaction transaction = new Transaction();

        // you can either use the index (starts from 0) or custom names
        transaction.setUsername(fieldSet.readString("username"));
        transaction.setUserId(fieldSet.readInt("userid"));
        transaction.setAmount(fieldSet.readDouble("amount"));

        // Converting the date
        String dateString = fieldSet.readString("transactiondate");
        transaction.setTransactionDate(LocalDate.parse(dateString, DATE_FORMATTER).atStartOfDay());

        return transaction;
    }
}
