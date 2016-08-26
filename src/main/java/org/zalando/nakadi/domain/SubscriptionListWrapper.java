package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class SubscriptionListWrapper {

    private final List<Subscription> items;

    public SubscriptionListWrapper(final List<Subscription> items) {
        this.items = items;
    }

    public List<Subscription> getItems() {
        return Collections.unmodifiableList(items);
    }
}
