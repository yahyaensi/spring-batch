package com.batch.model;

import java.time.LocalDate;
import java.time.LocalDateTime;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor(staticName = "of")
@NoArgsConstructor
@ToString
public class TransactionVO {

    private long id;
    private LocalDate transactionDate;
    private long accountId;
    private double amount;
    private LocalDateTime createdAt;
}
