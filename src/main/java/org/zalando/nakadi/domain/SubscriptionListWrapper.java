package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class SubscriptionListWrapper extends ItemsWrapper<Subscription> {

    @JsonProperty("_links")
    private final PaginationLinks links;

    public SubscriptionListWrapper(final List<Subscription> items, final PaginationLinks links) {
        super(items);
        this.links = links;
    }

    public PaginationLinks getLinks() {
        return links;
    }
}
