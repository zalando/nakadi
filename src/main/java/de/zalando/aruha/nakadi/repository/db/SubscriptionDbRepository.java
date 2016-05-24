package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

public class SubscriptionDbRepository extends AbstractDbRepository {

    private final SubscriptionMapper rowMapper = new SubscriptionMapper();

    public SubscriptionDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        super(jdbcTemplate, objectMapper);
    }

    public void saveSubscription(final Subscription subscription)
            throws InternalNakadiException, DuplicatedSubscriptionException {
        try {
            jdbcTemplate.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object) VALUES (?, ?::jsonb)",
                    subscription.getId(),
                    jsonMapper.writer().writeValueAsString(subscription));
        } catch (JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type", e);
        } catch (DuplicateKeyException e) {
            throw new DuplicatedSubscriptionException("Subscription with the same key properties already exists", e);
        }
    }

    public Subscription getSubscription(final String id) throws NoSuchEventTypeException {
        final String sql = "SELECT s_subscription_object FROM zn_data.subscription WHERE s_id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{id}, rowMapper);
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchEventTypeException("Subscription with id \"" + id + "\" does not exist.", e);
        }
    }

    public Subscription getSubscription(final String owningApplication, final List<String> eventTypes,
                                        final String useCase) throws NoSuchEventTypeException {
        final String sql = "SELECT s_subscription_object FROM zn_data.subscription " +
                "WHERE s_subscription_object->>'owning_application' = ? " +
                "AND s_subscription_object->>'event_types' = ? " +
                "AND s_subscription_object->>'use_case' = ? ";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{owningApplication, eventTypes, useCase}, rowMapper);
        } catch (EmptyResultDataAccessException e) {
            throw new NoSuchEventTypeException("Subscription does not exist.", e);
        }
    }

    private class SubscriptionMapper implements RowMapper<Subscription> {
        @Override
        public Subscription mapRow(ResultSet rs, int rowNum) throws SQLException {
            try {
                return jsonMapper.readValue(rs.getString("s_subscription_object"), Subscription.class);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
    }

}
