package com.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.batch.App;
import com.batch.model.Transaction;
import com.batch.service.exception.MissingUsernameException;
import com.batch.service.exception.NegativeAmountException;

public class SkippingItemProcessor implements ItemProcessor<Transaction, Transaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    @Override
    public Transaction process(Transaction transaction) {

        LOGGER.info("SkippingItemProcessor: {}", transaction);

        if (transaction.getUsername() == null || transaction.getUsername().isEmpty()) {
            throw new MissingUsernameException();
        }

        double txAmount = transaction.getAmount();
        if (txAmount < 0) {
            throw new NegativeAmountException(txAmount);
        }

        return transaction;
    }
}
