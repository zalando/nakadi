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
import org.zalando.nakadi.exceptions.runtime.DuplicatedSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.util.HashGenerator;
import org.zalando.nakadi.util.UUIDGenerator;

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

    private final RowMapper<Subscription> rowMapper;
    private final UUIDGenerator uuidGenerator;
    private final HashGenerator hashGenerator;

    @Autowired
    public SubscriptionDbRepository(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper,
                                    final UUIDGenerator uuidGenerator, final HashGenerator hashGenerator) {
        super(jdbcTemplate, jsonMapper);
        this.uuidGenerator = uuidGenerator;
        this.hashGenerator = hashGenerator;
        this.rowMapper = new SubscriptionMapper(jsonMapper);
    }

    public Subscription createSubscription(final SubscriptionBase subscriptionBase)
            throws InconsistentStateException, DuplicatedSubscriptionException, RepositoryProblemException {

        try {
            final String newId = uuidGenerator.randomUUID().toString();
            final String keyFieldsHash = hashGenerator.generateSubscriptionKeyFieldsHash(subscriptionBase);
            final DateTime createdAt = new DateTime(DateTimeZone.UTC);
            final Subscription subscription = new Subscription(newId, createdAt, createdAt, subscriptionBase);

            jdbcTemplate.update("INSERT INTO zn_data.subscription (s_id, s_subscription_object, s_key_fields_hash) " +
                            "VALUES (?, ?::JSONB, ?)",
                    subscription.getId(),
                    jsonMapper.writer().writeValueAsString(subscription),
                    keyFieldsHash);

            return subscription;
        } catch (final JsonProcessingException e) {
            throw new InconsistentStateException("Serialization problem during persistence of subscription", e);
        } catch (final DuplicateKeyException e) {
            throw new DuplicatedSubscriptionException("Subscription with the same key properties already exists", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when running database request", e);
        }
    }

    public void updateSubscription(final Subscription subscription) {
        final String keyFieldsHash = hashGenerator.generateSubscriptionKeyFieldsHash(subscription);
        try {
            jdbcTemplate.update(
                    "UPDATE zn_data.subscription set s_subscription_object=?::JSONB, s_key_fields_hash=? WHERE s_id=?",
                    jsonMapper.writer().writeValueAsString(subscription),
                    keyFieldsHash,
                    subscription.getId());
        } catch (final JsonProcessingException ex) {
            throw new InconsistentStateException("Serialization problem during persistence of subscription", ex);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when running database request", e);
        }
    }

    public Subscription getSubscription(final String id) throws NoSuchSubscriptionException,
            ServiceTemporarilyUnavailableException {
        final String sql = "SELECT s_subscription_object FROM zn_data.subscription WHERE s_id = ?";
        try {
            return jdbcTemplate.queryForObject(sql, new Object[]{id}, rowMapper);
        } catch (final EmptyResultDataAccessException e) {
            throw NoSuchSubscriptionException.withSubscriptionId(id, e);
        } catch (final DataAccessException e) {
            LOG.error("Database error when getting subscription", e);
            throw new ServiceTemporarilyUnavailableException("Error occurred when running database request");
        }
    }

    public void deleteSubscription(final String id)
            throws NoSuchSubscriptionException, ServiceTemporarilyUnavailableException {
        try {
            final int rowsDeleted = jdbcTemplate.update("DELETE FROM zn_data.subscription WHERE s_id = ?", id);
            if (rowsDeleted == 0) {
                throw NoSuchSubscriptionException.withSubscriptionId(id, null);
            }
        } catch (final DataAccessException e) {
            LOG.error("Database error when deleting subscription", e);
            throw new ServiceTemporarilyUnavailableException("Error occurred when running database request");
        }
    }

    /**
     * Use {@code SubscriptionTokenLister} instead
     */
    @Deprecated
    public List<Subscription> listSubscriptions(final Set<String> eventTypes, final Optional<String> owningApplication,
                                                final Optional<AuthorizationAttribute> reader,
                                                final int offset, final int limit)
            throws ServiceTemporarilyUnavailableException {

        final StringBuilder queryBuilder = new StringBuilder("SELECT s_subscription_object FROM zn_data.subscription");
        if (reader.isPresent()) {
            queryBuilder.append(",jsonb_to_recordset(s_subscription_object->'authorization'->'readers')" +
                    " as readers(data_type text, value text) ");
        }
        final List<String> clauses = Lists.newArrayList();
        final List<Object> params = Lists.newArrayList();

        applyFilter(eventTypes, owningApplication, reader, clauses, params);

        final String order = " ORDER BY s_subscription_object->>'created_at' DESC LIMIT ? OFFSET ? ";
        params.add(limit);
        params.add(offset);
        if (!clauses.isEmpty()) {
            queryBuilder.append(" WHERE ");
            queryBuilder.append(StringUtils.join(clauses, " AND "));
        }
        queryBuilder.append(order);
        try {
            return jdbcTemplate.query(queryBuilder.toString(), params.toArray(), rowMapper);
        } catch (final DataAccessException e) {
            LOG.error("Database error when listing subscriptions", e);
            throw new ServiceTemporarilyUnavailableException("Error occurred when running database request");
        }
    }

    static void applyFilter(
            final Set<String> eventTypes,
            final Optional<String> owningApplication,
            final Optional<AuthorizationAttribute> reader,
            final List<String> clauses,
            final List<Object> params) {
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
        if(reader.isPresent()){
            clauses.add(" readers.data_type = ? AND readers.value = ? ");
            params.add(reader.get().getDataType());
            params.add(reader.get().getValue());
        }
    }

    public Subscription getSubscription(final String owningApplication, final Set<String> eventTypes,
                                        final String consumerGroup)
            throws InconsistentStateException, NoSuchSubscriptionException, RepositoryProblemException {

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
            throw new NoSuchSubscriptionException("Subscription does not exist", e);
        } catch (final DataAccessException e) {
            throw new RepositoryProblemException("Error occurred when reading subscription from DB", e);
        }
    }

}
