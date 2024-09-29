package com.batch.service.exception;

public class NegativeAmountException extends RuntimeException {

    private static final long serialVersionUID = -2328357459525994837L;

    private double amount;

    public NegativeAmountException(double amount) {
        this.amount = amount;
    }

    public double getAmount() {
        return amount;
    }
}
