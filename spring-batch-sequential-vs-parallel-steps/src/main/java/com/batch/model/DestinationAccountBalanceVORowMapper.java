package com.batch.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class DestinationAccountBalanceVORowMapper implements RowMapper<DestinationAccountBalanceVO> {

    @Override
    public DestinationAccountBalanceVO mapRow(ResultSet rs, int rowNum) throws SQLException {
        return DestinationAccountBalanceVO.of(rs.getLong("account_id"),
                                              rs.getDouble("balance"));
    }
}
