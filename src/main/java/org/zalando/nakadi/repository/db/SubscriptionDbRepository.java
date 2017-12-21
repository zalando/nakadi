package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.util.HashGenerator;
import org.zalando.nakadi.util.UUIDGenerator;

import java.io.IOException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Sets.newTreeSet;
import static java.text.MessageFormat.format;

@Component
@Profile("!test")
public class SubscriptionDbRepository extends AbstractDbRepository {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionDbRepository.class);

    private final SubscriptionMapper rowMapper = new SubscriptionMapper();
    private final UUIDGenerator uuidGenerator;
    private final HashGenerator hashGenerator;

    @Autowired
    public SubscriptionDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper,
                                    final UUIDGenerator uuidGenerator, final HashGenerator hashGenerator) {
        super(jdbcTemplate, jsonMapper);
        this.uuidGenerator = uuidGenerator;
        this.hashGenerator = hashGenerator;
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase)
            throws InconsistentStateException, DuplicatedSubscriptionException, RepositoryProblemException {

        try {
            final String newId = uuidGenerator.randomUUID().toString();
            final String keyFieldsHash = hashGenerator.generateSubscriptionKeyFieldsHash(subscriptionBase);
            final DateTime createdAt = new DateTime(DateTimeZone.UTC);
            final Subscription subscription = new Subscription(newId, createdAt, subscriptionBase);

            jdbcTemplate.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object, s_key_fields_hash) " +
                            "VALUES (?, ?::JSONB, ?)",
                    subscription.getId(),
                    jsonMapper.writer().writeValueAsString(subscription),
                    keyFieldsHash);

            return subscription;
        } catch (final JsonProcessingException e) {
            throw new InconsistentStateException("Serialization problem during persistence of event type", e);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedSubscriptionException("Subscription with the same key properties already exists", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when running database request", e);
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
            LOG.error("Database error when getting subscription: {}", e.getMessage());
            throw new ServiceUnavailableException("Error occurred when running database request");
        }
    }

    public void deleteSubscription(final String id) throws NoSuchSubscriptionException, ServiceUnavailableException {
        try {
            final int rowsDeleted = jdbcTemplate.update("DELETE FROM zn_data.subscription WHERE s_id = ?", id);
            if (rowsDeleted == 0) {
                throw new NoSuchSubscriptionException("Subscription with id \"" + id + "\" does not exist");
            }
        } catch (final DataAccessException e) {
            LOG.error("Database error when deleting subscription: {}", e.getMessage());
            throw new ServiceUnavailableException("Error occurred when running database request");
        }
    }

    public List<Subscription> listSubscriptions(final Set<String> eventTypes, final Optional<String> owningApplication,
                                                final int offset, final int limit) throws ServiceUnavailableException {

        final StringBuilder queryBuilder = new StringBuilder("SELECT s_subscription_object FROM zn_data.subscription ");
        final List<String> clauses = Lists.newArrayList();
        final List<Object> params = Lists.newArrayList();

        owningApplication.ifPresent(owningApp -> {
            clauses.add(" s_subscription_object->>'owning_application' = ? ");
            params.add(owningApp);
        });
        if (!eventTypes.isEmpty()) {
            final String clause = eventTypes.stream()
                    .map(et -> " s_subscription_object->'event_types' @> ?::jsonb")
                    .collect(Collectors.joining(" AND "));
            clauses.add(clause);
            eventTypes.stream()
                    .map(et -> format("\"{0}\"", et))
                    .forEach(params::add);
        }
        if (!clauses.isEmpty()) {
            queryBuilder.append(" WHERE ");
            queryBuilder.append(StringUtils.join(clauses, " AND "));
        }

        queryBuilder.append(" ORDER BY s_subscription_object->>'created_at' DESC LIMIT ? OFFSET ? ");
        params.add(limit);
        params.add(offset);
        try {
            return jdbcTemplate.query(queryBuilder.toString(), params.toArray(), rowMapper);
        } catch (final DataAccessException e) {
            LOG.error("Database error when listing subscriptions: {}", e.getMessage());
            throw new ServiceUnavailableException("Error occurred when running database request");
        }
    }

    public Subscription getSubscription(final String owningApplication, final Set<String> eventTypes,
                                        final String consumerGroup)
            throws InconsistentStateException, NoSubscriptionException, RepositoryProblemException {

        final String sql = "SELECT s_subscription_object FROM zn_data.subscription " +
                "WHERE s_subscription_object->>'owning_application' = ? " +
                "AND replace(s_subscription_object->>'event_types', ' ', '') = ? " +
                "AND s_subscription_object->>'consumer_group' = ? ";
        try {
            final String eventTypesJson = jsonMapper.writer().writeValueAsString(newTreeSet(eventTypes));
            return jdbcTemplate.queryForObject(sql, new Object[]{owningApplication, eventTypesJson, consumerGroup},
                    rowMapper);
        } catch (final JsonProcessingException e) {
            throw new InconsistentStateException("Deserialization problem during reading subscription from DB", e);
        } catch (final EmptyResultDataAccessException e) {
            throw new NoSubscriptionException("Subscription does not exist", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when reading subscription from DB", e);
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
