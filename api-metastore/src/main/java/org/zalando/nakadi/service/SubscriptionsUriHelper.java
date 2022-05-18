package org.zalando.nakadi.service;

import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.PaginationLinks;
import org.zalando.nakadi.model.AuthorizationAttributeQueryParser;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;

import java.util.Optional;
import java.util.Set;

public class SubscriptionsUriHelper {

    public static PaginationLinks.Link createSubscriptionListLink(
            final Optional<String> owningApplication, final Set<String> eventTypes,
            final Optional<AuthorizationAttribute> reader, final int offset,
            final Optional<SubscriptionDbRepository.Token> token, final int limit, final boolean showStatus) {

        final UriComponentsBuilder urlBuilder = UriComponentsBuilder.fromPath("/subscriptions");
        if (!eventTypes.isEmpty()) {
            urlBuilder.queryParam("event_type", eventTypes.toArray());
        }
        reader.ifPresent(readerOpt -> {
            urlBuilder.queryParam("reader", AuthorizationAttributeQueryParser.getQuery(readerOpt));
        });
        owningApplication.ifPresent(owningApp -> urlBuilder.queryParam("owning_application", owningApp));
        if (showStatus) {
            urlBuilder.queryParam("show_status", "true");
        }
        if (token.isPresent()) {
            urlBuilder.queryParam("token", token.get().encode());
        } else {
            urlBuilder.queryParam("offset", offset);
        }
        return new PaginationLinks.Link(urlBuilder
                .queryParam("limit", limit)
                .build()
                .toString());
    }
}
