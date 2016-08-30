package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class SubscriptionListWrapper {

    private final List<Subscription> items;

    @JsonProperty("_links")
    private final PaginationLinks links;

    public SubscriptionListWrapper(final List<Subscription> items, final PaginationLinks links) {
        this.items = items;
        this.links = links;
    }

    public List<Subscription> getItems() {
        return Collections.unmodifiableList(items);
    }

    public PaginationLinks getLinks() {
        return links;
    }
}
