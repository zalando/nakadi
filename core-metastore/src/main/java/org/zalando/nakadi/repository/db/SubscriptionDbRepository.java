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

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
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
     * Internal use only.
     */
    public List<Subscription> listAllSubscriptionsFor(final Set<String> eventTypes)
            throws ServiceTemporarilyUnavailableException {

        return listSubscriptions(eventTypes, Optional.empty(), Optional.empty(), Optional.empty());
    }

    /**
     * Use {@code listSubscriptions} with token instead.
     */
    @Deprecated
    public List<Subscription> listSubscriptions(final Set<String> eventTypes, final Optional<String> owningApplication,
                                                final Optional<AuthorizationAttribute> reader,
                                                final Optional<PaginationParameters> paginationParams)
            throws ServiceTemporarilyUnavailableException {

        final List<String> clauses = Lists.newArrayList();
        final List<Object> params = Lists.newArrayList();
        final StringBuilder query = initQuery(eventTypes, owningApplication, reader, clauses, params);

        appendClauses(query, clauses);

        query.append(" ORDER BY s_subscription_object->>'created_at' DESC");

        paginationParams.ifPresent(pp -> {
            query.append(" LIMIT ? OFFSET ?");
            params.add(pp.limit);
            params.add(pp.offset);
        });

        return executeListSubscriptions(query.toString(), params);
    }

    /**
     * Method lists subscriptions.
     *
     * @param eventTypes        Event types that are targeted by subscription
     * @param owningApplication Ownnig application of subscriptions
     * @param reader            Authorization attribute of a reader of subscripitons
     * @param token             Token to use for iteration
     * @param limit             Amount of records to return. Min value is 1
     * @return List of subscriptions with optional tokens to iterate forward and backward
     */
    public ListResult listSubscriptions(
            final Set<String> eventTypes,
            final Optional<String> owningApplication,
            final Optional<AuthorizationAttribute> reader,
            @Nullable final Token token,
            final int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit can't be less than 1");
        }

        final List<String> clauses = Lists.newArrayList();
        final List<Object> params = Lists.newArrayList();
        final StringBuilder query = initQuery(eventTypes, owningApplication, reader, clauses, params);

        if (null == token || !token.hasSubscriptionId()) {
            return listNoToken(query, clauses, params, limit);
        } else {
            clauses.add(" s_id " + token.getTokenType().dbClause + " ? ");
            params.add(token.getSubscriptionId());

            if (token.getTokenType().forward) {
                return listForwardToken(query, clauses, params, token, limit);
            } else {
                return listBackwardToken(query, clauses, params, token, limit);
            }
        }
    }

    private static StringBuilder initQuery(
            final Set<String> eventTypes,
            final Optional<String> owningApplication,
            final Optional<AuthorizationAttribute> reader,
            final List<String> clauses,
            final List<Object> params) {

        final StringBuilder query = new StringBuilder("SELECT s_subscription_object FROM zn_data.subscription");

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

        if (reader.isPresent()) {
            query.append(", jsonb_to_recordset(s_subscription_object->'authorization'->'readers')" +
                    " AS readers(data_type text, value text) ");

            clauses.add(" readers.data_type = ? AND readers.value = ? ");
            params.add(reader.get().getDataType());
            params.add(reader.get().getValue());
        }

        return query;
    }

    private static void appendClauses(final StringBuilder query, final List<String> clauses) {
        if (!clauses.isEmpty()) {
            query.append(" WHERE ");
            query.append(String.join(" AND ", clauses));
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

    public static class PaginationParameters {
        public final int limit;
        public final int offset;

        public PaginationParameters(final int limit, final int offset){
            this.limit = limit;
            this.offset = offset;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final PaginationParameters that = (PaginationParameters) o;
            return limit == that.limit && offset == that.offset;
        }

        @Override
        public int hashCode() {
            return Objects.hash(limit, offset);
        }

        @Override
        public String toString() {
            return "PaginationParameters{" +
                    "limit=" + limit +
                    ", offset=" + offset +
                    '}';
        }
    }

    private ListResult listBackwardToken(
            final StringBuilder query, final List<String> clauses, final List<Object> params,
            final Token token, final int limit) {

        final List<Subscription> subscriptionsPlusOne = executeListSubscriptions(
                query, clauses, params, limit + 1, false);

        // subscriptions are listed in a backwards direction, therefore they should be reversed afterwards
        Collections.reverse(subscriptionsPlusOne);

        if (subscriptionsPlusOne.size() > limit) {
            // we need to remove the first one, as it shows that there is something before.
            return new ListResult(
                    subscriptionsPlusOne.subList(1, subscriptionsPlusOne.size()),
                    new Token(subscriptionsPlusOne.get(limit).getId(), TokenType.FORWARD),
                    new Token(subscriptionsPlusOne.get(0).getId(), TokenType.BACKWARD)
            );
        } else {
            // we have less than needed, that means that backward token is not available
            return new ListResult(
                    subscriptionsPlusOne,
                    new Token(
                            token.getSubscriptionId(),
                            token.getTokenType() == TokenType.BACKWARD ? TokenType.FORWARD_INCL : TokenType.FORWARD),
                    null
            );
        }
    }

    private ListResult listForwardToken(
            final StringBuilder query, final List<String> clauses, final List<Object> params,
            final Token token, final int limit) {

        final List<Subscription> subscriptionsPlusOne = executeListSubscriptions(
                query, clauses, params, limit + 1, true);

        if (subscriptionsPlusOne.size() > limit) {
            return new ListResult(
                    subscriptionsPlusOne.subList(0, limit),
                    new Token(subscriptionsPlusOne.get(limit).getId(), TokenType.FORWARD_INCL),
                    new Token(subscriptionsPlusOne.get(0).getId(), TokenType.BACKWARD));
        } else {
            return new ListResult(
                    subscriptionsPlusOne,
                    null,
                    new Token(
                            token.getSubscriptionId(),
                            token.getTokenType() == TokenType.FORWARD ? TokenType.BACKWARD_INCL : TokenType.BACKWARD));
        }
    }

    private ListResult listNoToken(
            final StringBuilder query, final List<String> clauses, final List<Object> params,
            final int limit) {

        final List<Subscription> subscriptionsPlusOne = executeListSubscriptions(
                query, clauses, params, limit + 1, true);

        if (subscriptionsPlusOne.size() > limit) {
            return new ListResult(
                    subscriptionsPlusOne.subList(0, limit),
                    new Token(subscriptionsPlusOne.get(limit).getId(), TokenType.FORWARD_INCL),
                    null
            );
        } else {
            return new ListResult(
                    subscriptionsPlusOne,
                    null,
                    null);
        }
    }

    private List<Subscription> executeListSubscriptions(
            final StringBuilder query, final List<String> clauses, final List<Object> params,
            final int limit, final boolean asc) {

        appendClauses(query, clauses);

        query.append(" ORDER by s_id ");
        query.append(asc ? "ASC" : "DESC");

        query.append(" LIMIT ?");
        params.add(limit);

        return executeListSubscriptions(query.toString(), params);
    }

    private List<Subscription> executeListSubscriptions(final String query, final List<Object> params) {
        try {
            return jdbcTemplate.query(query, params.toArray(), rowMapper);
        } catch (final DataAccessException e) {
            throw new ServiceTemporarilyUnavailableException("Error occurred when running database request", e);
        }
    }

    public static class ListResult {
        private final List<Subscription> items;
        private final Token next;
        private final Token prev;

        public ListResult(final List<Subscription> items, final Token next, final Token prev) {
            this.items = items;
            this.next = next;
            this.prev = prev;
        }

        public List<Subscription> getItems() {
            return items;
        }

        @Nullable
        public Token getNext() {
            return next;
        }

        @Nullable
        public Token getPrev() {
            return prev;
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final ListResult that = (ListResult) o;
            return Objects.equals(items, that.items) &&
                    Objects.equals(next, that.next) &&
                    Objects.equals(prev, that.prev);
        }

        @Override
        public int hashCode() {
            return Objects.hash(items, next, prev);
        }
    }

    public enum TokenType {
        FORWARD('F', ">", true),
        FORWARD_INCL('W', ">=", true),
        BACKWARD('B', "<", false),
        BACKWARD_INCL('I', "<=", false),
        ;
        private final char symbol;
        private final String dbClause;
        private final boolean forward;

        TokenType(final char symbol, final String dbClause, final boolean forward) {
            this.symbol = symbol;
            this.dbClause = dbClause;
            this.forward = forward;
        }

        public static TokenType of(final char symbol) {
            for (final TokenType tt : TokenType.values()) {
                if (tt.symbol == symbol) {
                    return tt;
                }
            }
            throw new IllegalArgumentException("Token type not supported");
        }
    }

    public static class Token {
        private final String subscriptionId;
        private final TokenType tokenType;

        public Token(final String subscriptionId, final TokenType tokenType) {
            this.subscriptionId = subscriptionId;
            this.tokenType = tokenType;
        }

        public boolean hasSubscriptionId() {
            return !StringUtils.isEmpty(subscriptionId);
        }

        public String getSubscriptionId() {
            return subscriptionId;
        }

        public TokenType getTokenType() {
            return tokenType;
        }

        public static Token createEmpty() {
            return new Token("", TokenType.FORWARD);
        }

        public String encode() {
            return tokenType.symbol + subscriptionId;
        }

        public static Token parse(final String value) {
            if (StringUtils.isEmpty(value) || value.length() < 1) {
                return createEmpty();
            }
            return new Token(value.substring(1), TokenType.of(value.charAt(0)));
        }

        @Override
        public boolean equals(final Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            final Token token = (Token) o;
            return Objects.equals(subscriptionId, token.subscriptionId) &&
                    tokenType == token.tokenType;
        }

        @Override
        public int hashCode() {
            return Objects.hash(subscriptionId, tokenType);
        }

        @Override
        public String toString() {
            return "Token{" +
                    "subscriptionId='" + subscriptionId + '\'' +
                    ", tokenType=" + tokenType +
                    '}';
        }
    }
}
