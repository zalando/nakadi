package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.jdbc.core.RowMapper;
import org.zalando.nakadi.domain.Subscription;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;

class SubscriptionMapper implements RowMapper<Subscription> {
    private final ObjectMapper objectMapper;

    SubscriptionMapper(final ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    @Override
    public Subscription mapRow(final ResultSet rs, final int rowNum) throws SQLException {
        try {
            return objectMapper.readValue(rs.getString("s_subscription_object"), Subscription.class);
        } catch (final IOException e) {
            throw new SQLException(e);
        }
    }
}
