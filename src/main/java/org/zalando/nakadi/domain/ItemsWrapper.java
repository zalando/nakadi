package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class ItemsWrapper<T> {

    private final List<T> items;

    @JsonCreator
    public ItemsWrapper(@JsonProperty("items") final List<T> items) {
        this.items = items;
    }

    public List<T> getItems() {
        return Collections.unmodifiableList(items);
    }
}
