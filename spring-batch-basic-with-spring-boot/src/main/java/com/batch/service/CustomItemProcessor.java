package com.batch.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import com.batch.App;
import com.batch.model.Transaction;

public class CustomItemProcessor implements ItemProcessor<Transaction, Transaction> {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);

    @Override
    public Transaction process(Transaction item) {
        LOGGER.info("Processing...{}", item);
        return item;
    }
}