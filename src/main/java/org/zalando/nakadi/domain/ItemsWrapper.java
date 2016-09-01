package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class ItemsWrapper<T> {

    private final List<T> items;

    @JsonProperty("_links")
    private final PaginationLinks links;

    public ItemsWrapper(final List<T> items, final PaginationLinks links) {
        this.items = items;
        this.links = links;
    }

    public List<T> getItems() {
        return Collections.unmodifiableList(items);
    }

    public PaginationLinks getLinks() {
        return links;
    }
}
