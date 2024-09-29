package com.batch.model;

import java.sql.ResultSet;
import java.sql.SQLException;

import org.springframework.jdbc.core.RowMapper;

public class SourceAccountVORowMapper implements RowMapper<SourceAccountVO> {

    @Override
    public SourceAccountVO mapRow(ResultSet rs, int rowNum) throws SQLException {
        return SourceAccountVO.of(rs.getLong("id"),
                                  rs.getString("account_number"),
                                  rs.getTimestamp("created_at").toLocalDateTime());
    }
}
