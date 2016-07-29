package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.dao.DataAccessException;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.util.UUIDGenerator;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newTreeSet;

@Component
@Profile("!test")
public class SubscriptionDbRepository extends AbstractDbRepository {

    private final SubscriptionMapper rowMapper = new SubscriptionMapper();
    private final UUIDGenerator uuidGenerator;

    @Autowired
    public SubscriptionDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper objectMapper,
                                    final UUIDGenerator uuidGenerator) {
        super(jdbcTemplate, objectMapper);
        this.uuidGenerator = uuidGenerator;
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase) throws InternalNakadiException,
            DuplicatedSubscriptionException, ServiceUnavailableException {

        try {
            final String newId = uuidGenerator.randomUUID().toString();
            final DateTime createdAt = new DateTime(DateTimeZone.UTC);
            final Subscription subscription = new Subscription(newId, createdAt, subscriptionBase);

            jdbcTemplate.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object) VALUES (?, ?::JSONB)",
                    subscription.getId(),
                    jsonMapper.writer().writeValueAsString(subscription));

            return subscription;
        } catch (final JsonProcessingException e) {
            throw new InternalNakadiException("Serialization problem during persistence of event type", e);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedSubscriptionException("Subscription with the same key properties already exists", e);
        } catch (final DataAccessException e) {
            throw new ServiceUnavailableException("Error occurred when running database request", e);
        }
    }

    public Subscription getSubscription(final String id) throws NoSuchSubscriptionException,
            ServiceUnavailableException {
        final String sql = "SELECT s_subscription_object FROM zn_data.subscription WHERE s_id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{id}, rowMapper);
        } catch (final EmptyResultDataAccessException e) {
            throw new NoSuchSubscriptionException("Subscription with id \"" + id + "\" does not exist", e);
        } catch (final DataAccessException e) {
            throw new ServiceUnavailableException("Error occurred when running database request", e);
        }
    }

    public List<Subscription> listSubscriptions() throws ServiceUnavailableException {
        try {
            return jdbcTemplate.query("SELECT s_subscription_object FROM zn_data.subscription", rowMapper);
        } catch (final DataAccessException e) {
            throw new ServiceUnavailableException("Error occurred when running database request", e);
        }
    }

    public List<Subscription> listSubscriptionsForOwningApplication(final String owningApplication)
            throws ServiceUnavailableException {
        final String query = "SELECT s_subscription_object FROM zn_data.subscription " +
                "WHERE s_subscription_object->>'owning_application' = ?";
        try {
            return jdbcTemplate.query(query, new Object[]{owningApplication}, rowMapper);
        } catch (final DataAccessException e) {
            throw new ServiceUnavailableException("Error occurred when running database request", e);
        }
    }

    public Subscription getSubscription(final String owningApplication, final Set<String> eventTypes,
                                        final String consumerGroup)
            throws NoSuchSubscriptionException, InternalNakadiException, ServiceUnavailableException {

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
        } catch (final DataAccessException e) {
            throw new ServiceUnavailableException("Error occurred when running database request", e);
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
