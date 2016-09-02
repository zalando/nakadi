package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class ItemsWrapper<T> {

    private final List<T> items;

    public ItemsWrapper(final List<T> items) {
        this.items = items;
    }

    public List<T> getItems() {
        return Collections.unmodifiableList(items);
    }
}
