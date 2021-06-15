package org.zalando.nakadi.repository.db;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

@Service
public class SubscriptionTokenLister extends AbstractDbRepository {
    private final RowMapper<Subscription> rowMapper;

    @Autowired
    public SubscriptionTokenLister(final JdbcTemplate jdbcTemplate, final ObjectMapper jsonMapper) {
        super(jdbcTemplate, jsonMapper);
        this.rowMapper = new SubscriptionMapper(jsonMapper);
    }

    /**
     * Method lists subscriptions.
     *
     * @param eventTypes        Event types that are targeted by subscription
     * @param owningApplication Ownnig application of subscriptions
     * @param token             Token to use for iteration
     * @param limit             Amount of records to return. Min value is 1
     * @return List of subscriptions with optional tokens to iterate forward and backward
     */
    public ListResult listSubscriptions(
            final Set<String> eventTypes,
            final Optional<String> owningApplication,
            final Set<String> readers,
            @Nullable final Token token,
            final int limit) {
        if (limit < 1) {
            throw new IllegalArgumentException("Limit can't be less than 1");
        }
        final List<String> clauses = Lists.newArrayList();
        final List<Object> params = Lists.newArrayList();
        SubscriptionDbRepository.applyFilter(eventTypes, owningApplication, readers, clauses, params);

        if (null == token || !token.hasSubscriptionId()) {
            return listNoToken(clauses, params, limit);
        } else if (token.getTokenType().forward) {
            return listForwardToken(clauses, params, token, limit);
        } else {
            return listBackwardToken(clauses, params, token, limit);
        }
    }

    private ListResult listBackwardToken(
            final List<String> clauses, final List<Object> params,
            final Token token, final int limit) {
        clauses.add(" s_id " + token.getTokenType().dbClause + " ? ");
        params.add(token.getSubscriptionId());
        final List<Subscription> subscriptionsPlusOne = execute(clauses, params, limit + 1, false);
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
            final List<String> clauses, final List<Object> params,
            final Token token, final int limit) {
        clauses.add(" s_id " + token.getTokenType().dbClause + " ? ");
        params.add(token.getSubscriptionId());
        final List<Subscription> subscriptionsPlusOne = execute(clauses, params, limit + 1, true);
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
            final List<String> clauses,
            final List<Object> params,
            final int limit) {
        final List<Subscription> subscriptionsPlusOne = execute(clauses, params, limit + 1, true);
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

    public List<Subscription> execute(final List<String> clauses, final List<Object> params,
                                      final int limit, final boolean asc) {
        final StringBuilder queryBuilder = new StringBuilder("SELECT s_subscription_object FROM zn_data.subscription ");

        if (!clauses.isEmpty()) {
            queryBuilder.append(" WHERE ");
            queryBuilder.append(StringUtils.join(clauses, " AND "));
        }
        queryBuilder.append(" ORDER by s_id ");
        queryBuilder.append(asc ? "ASC" : "DESC");
        queryBuilder.append(" LIMIT ?");
        // do not modify input.
        final List<Object> paramsCopy = new ArrayList<>(params);
        paramsCopy.add(limit);
        try {
            return jdbcTemplate.query(queryBuilder.toString(), paramsCopy.toArray(), rowMapper);
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
