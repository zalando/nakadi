package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractDbRepository {

    protected final JdbcTemplate jdbcTemplate;
    protected final ObjectMapper jsonMapper;

    public AbstractDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        this.jdbcTemplate = jdbcTemplate;
        this.jsonMapper = jsonMapper;
    }

}
