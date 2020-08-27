package org.zalando.nakadi.service;

import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;

import java.util.List;
import java.util.Optional;
import java.util.Set;

public class SubscriptionsUriHelper {

    public static PaginationLinks createSubscriptionPaginationLinks(
            final Optional<String> owningApplication,
            final Set<String> eventTypes,
            final int offset,
            final int limit,
            final boolean showStatus,
            final List<Subscription> subscriptions,
            final SubscriptionDbRepository.Token oldToken) {
        Optional<PaginationLinks.Link> prevLink = Optional.empty();
        if (null != oldToken) {
            final SubscriptionDbRepository.Token token;
            if (subscriptions.isEmpty()) {
                // The trick here is that we may own old token, that is pointing to non-existent position.
                // in this case we may generate response with empty result (and it is fine)
                if (oldToken.hasSubscriptionId()
                        && oldToken.getTokenType() == SubscriptionDbRepository.TokenType.FORWARD) {
                    token = new SubscriptionDbRepository.Token(
                            oldToken.getSubscriptionId(),
                            SubscriptionDbRepository.TokenType.BACKWARD_INCL);
                } else {
                    token = null;
                }
            } else {
                if (oldToken.hasSubscriptionId()) {
                    token = new SubscriptionDbRepository.Token(
                            subscriptions.get(0).getId(), SubscriptionDbRepository.TokenType.BACKWARD);
                } else {
                    token = null;
                }
            }
            if (null != token) {
                final String prevUri = createSubscriptionListUri(
                        owningApplication, eventTypes, 0, limit, showStatus, token.encode());
                prevLink = Optional.of(new PaginationLinks.Link(prevUri));
            }
        } else if (offset > 0) {
            final int newOffset = offset >= limit ? offset - limit : 0;
            final String prevUri = createSubscriptionListUri(owningApplication, eventTypes,
                    newOffset, limit, showStatus, null);
            prevLink = Optional.of(new PaginationLinks.Link(prevUri));
        }

        Optional<PaginationLinks.Link> nextLink = Optional.empty();
        if (subscriptions.size() >= limit && limit > 0) {
            final String nextUri;
            if (null != oldToken) {
                final SubscriptionDbRepository.Token token = new SubscriptionDbRepository.Token(
                        subscriptions.get(subscriptions.size() - 1).getId(),
                        SubscriptionDbRepository.TokenType.FORWARD);
                nextUri = createSubscriptionListUri(
                        owningApplication, eventTypes, 0, limit, showStatus, token.encode());
            } else {
                nextUri = createSubscriptionListUri(owningApplication, eventTypes, offset + limit,
                        limit, showStatus, null);
            }
            nextLink = Optional.of(new PaginationLinks.Link(nextUri));
        }

        return new PaginationLinks(prevLink, nextLink);
    }

    public static String createSubscriptionListUri(final Optional<String> owningApplication,
                                                   final Set<String> eventTypes, final int offset, final int limit,
                                                   final boolean showStatus, final String token) {

        final UriComponentsBuilder urlBuilder = UriComponentsBuilder.fromPath("/subscriptions");
        if (!eventTypes.isEmpty()) {
            urlBuilder.queryParam("event_type", eventTypes.toArray());
        }
        owningApplication.ifPresent(owningApp -> urlBuilder.queryParam("owning_application", owningApp));
        if (showStatus) {
            urlBuilder.queryParam("show_status", "true");
        }
        if (null != token && !token.isEmpty()) {
            urlBuilder.queryParam("token", token);
        } else {
            urlBuilder.queryParam("offset", offset);
        }
        return urlBuilder
                .queryParam("limit", limit)
                .build()
                .toString();
    }
}
