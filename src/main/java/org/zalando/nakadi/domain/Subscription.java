package org.zalando.nakadi.domain;

import org.joda.time.DateTime;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@EqualsAndHashCode(callSuper = true)
public class Subscription extends SubscriptionBase {

    public Subscription() {
        super();
    }

    public Subscription(final String id, final DateTime createdAt, final SubscriptionBase subscriptionBase) {
        super(subscriptionBase);
        this.id = id;
        this.createdAt = createdAt;
    }

    private String id;

    private DateTime createdAt;
}
