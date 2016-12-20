package org.zalando.nakadi.domain;

import java.util.List;

import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.annotation.JsonProperty;

import lombok.Getter;

@Immutable
@Getter
public class SubscriptionListWrapper extends ItemsWrapper<Subscription> {

    @JsonProperty("_links")
    private final PaginationLinks links;

    public SubscriptionListWrapper(final List<Subscription> items, final PaginationLinks links) {
        super(items);
        this.links = links;
    }
}
