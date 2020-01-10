package org.zalando.nakadi.util;

import org.springframework.web.util.UriComponentsBuilder;
import org.zalando.nakadi.domain.PaginationLinks;

import java.util.Optional;
import java.util.Set;

public class SubscriptionsUriHelper {

    public static PaginationLinks createSubscriptionPaginationLinks(final Optional<String> owningApplication,
                                                                    final Set<String> eventTypes, final int offset,
                                                                    final int limit, final boolean showStatus,
                                                                    final int currentPageItemCount) {
        Optional<PaginationLinks.Link> prevLink = Optional.empty();
        if (offset > 0) {
            final int newOffset = offset >= limit ? offset - limit : 0;
            final String prevUri = createSubscriptionListUri(owningApplication, eventTypes,
                    newOffset, limit, showStatus);
            prevLink = Optional.of(new PaginationLinks.Link(prevUri));
        }

        Optional<PaginationLinks.Link> nextLink = Optional.empty();
        if (currentPageItemCount >= limit) {
            final String nextUri = createSubscriptionListUri(owningApplication, eventTypes, offset + limit,
                    limit, showStatus);
            nextLink = Optional.of(new PaginationLinks.Link(nextUri));
        }

        return new PaginationLinks(prevLink, nextLink);
    }

    public static String createSubscriptionListUri(final Optional<String> owningApplication,
                                                   final Set<String> eventTypes, final int offset, final int limit,
                                                   final boolean showStatus) {

        final UriComponentsBuilder urlBuilder = UriComponentsBuilder.fromPath("/subscriptions");
        if (!eventTypes.isEmpty()) {
            urlBuilder.queryParam("event_type", eventTypes.toArray());
        }
        owningApplication.ifPresent(owningApp -> urlBuilder.queryParam("owning_application", owningApp));
        if (showStatus) {
            urlBuilder.queryParam("show_status", "true");
        }
        return urlBuilder
                .queryParam("offset", offset)
                .queryParam("limit", limit)
                .build()
                .toString();
    }
}
