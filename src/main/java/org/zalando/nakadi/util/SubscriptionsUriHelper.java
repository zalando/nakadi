package org.zalando.nakadi.util;

import org.zalando.nakadi.domain.PaginationLinks;

import java.util.Optional;
import java.util.Set;

import static java.text.MessageFormat.format;

public class SubscriptionsUriHelper {

    public static PaginationLinks createSubscriptionPaginationLinks(final Optional<String> owningApplication,
                                                                    final Set<String> eventTypes, final int offset,
                                                                    final int limit, final int currentPageItemCount) {
        Optional<PaginationLinks.Link> prevLink = Optional.empty();
        if (offset > 0) {
            final String prevUri = createSubscriptionListUri(owningApplication, eventTypes, offset - 1, limit);
            prevLink = Optional.of(new PaginationLinks.Link(prevUri));
        }

        Optional<PaginationLinks.Link> nextLink = Optional.empty();
        if (currentPageItemCount >= limit) {
            final String nextUri = createSubscriptionListUri(owningApplication, eventTypes, offset + 1, limit);
            nextLink = Optional.of(new PaginationLinks.Link(nextUri));
        }

        return new PaginationLinks(prevLink, nextLink);
    }

    public static String createSubscriptionListUri(final Optional<String> owningApplication,
                                                   final Set<String> eventTypes, final int offset, final int limit) {
        final StringBuilder urlBuilder = new StringBuilder("/subscriptions?");
        if (!eventTypes.isEmpty()) {
            eventTypes.stream()
                    .map(et -> format("event_type={0}", et))
                    .forEach(et -> urlBuilder.append(et).append("&"));
        }
        owningApplication.ifPresent(owningApp ->
                urlBuilder
                        .append("owning_application=")
                        .append(owningApp)
                        .append("&"));
        urlBuilder
                .append("offset=")
                .append(offset)
                .append("&limit=")
                .append(limit);
        return urlBuilder.toString();
    }
}
