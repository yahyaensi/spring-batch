package com.batch.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class TransactionVORowMapper implements RowMapper<TransactionVO> {

    @Override
    public TransactionVO mapRow(ResultSet rs, int rowNum) throws SQLException {
        return TransactionVO.of(rs.getLong("id"),
                                rs.getDate("transaction_date").toLocalDate(),
                                rs.getLong("account_id"),
                                rs.getDouble("amount"),
                                rs.getTimestamp("created_at").toLocalDateTime());
    }
}
