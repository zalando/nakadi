package de.zalando.aruha.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.SubscriptionBase;
import de.zalando.aruha.nakadi.exceptions.DuplicatedSubscriptionException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Set;
import java.util.UUID;

import static com.google.common.collect.Sets.newTreeSet;

public class SubscriptionDbRepository extends AbstractDbRepository {

    private final SubscriptionMapper rowMapper = new SubscriptionMapper();

    public SubscriptionDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper) {
        super(jdbcTemplate, objectMapper);
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase) throws InternalNakadiException,
            DuplicatedSubscriptionException {

        try {
            final String newId = UUID.randomUUID().toString();
            final DateTime createdAt = new DateTime(DateTimeZone.UTC);
            final Subscription subscription = new Subscription(newId, createdAt, subscriptionBase);

            jdbcTemplate.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object) VALUES (?, ?::jsonb)",
                    subscription.getId(),
                    jsonMapper.writer().writeValueAsString(subscription));

            return subscription;
        } catch (final JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type", e);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedSubscriptionException("Subscription with the same key properties already exists", e);
        }
    }

    public Subscription getSubscription(final String id) throws NoSuchSubscriptionException {
        final String sql = "SELECT s_subscription_object FROM zn_data.subscription WHERE s_id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{id}, rowMapper);
        } catch (final EmptyResultDataAccessException e) {
            throw new NoSuchSubscriptionException("Subscription with id \"" + id + "\" does not exist", e);
        }
    }

    public Subscription getSubscription(final String owningApplication, final Set<String> eventTypes,
                                        final String consumerGroup)
            throws NoSuchSubscriptionException, InternalNakadiException {

        final String sql = "SELECT s_subscription_object FROM zn_data.subscription " +
                "WHERE s_subscription_object->>'owning_application' = ? " +
                "AND replace(s_subscription_object->>'event_types', ' ', '') = ? " +
                "AND s_subscription_object->>'consumer_group' = ? ";
        try {
            final String eventTypesJson = jsonMapper.writer().writeValueAsString(newTreeSet(eventTypes));
            return jdbcTemplate.queryForObject(sql, new Object[]{owningApplication, eventTypesJson, consumerGroup},
                    rowMapper);
        } catch (final JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during getting event type", e);
        } catch (final EmptyResultDataAccessException e) {
            throw new NoSuchSubscriptionException("Subscription does not exist", e);
        }
    }

    private class SubscriptionMapper implements RowMapper<Subscription> {
        @Override
        public Subscription mapRow(final ResultSet rs, final int rowNum) throws SQLException {
            try {
                return jsonMapper.readValue(rs.getString("s_subscription_object"), Subscription.class);
            } catch (final IOException e) {
                throw new SQLException(e);
            }
        }
    }

}
