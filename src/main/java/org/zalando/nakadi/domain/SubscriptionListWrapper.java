package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class SubscriptionListWrapper {

    private final List<Subscription> data;

    public SubscriptionListWrapper(final List<Subscription> data) {
        this.data = data;
    }

    public List<Subscription> getData() {
        return Collections.unmodifiableList(data);
    }
}
