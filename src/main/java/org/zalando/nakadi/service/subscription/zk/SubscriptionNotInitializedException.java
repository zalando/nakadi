package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;

public class SubscriptionNotInitializedException extends NakadiBaseException {

    public SubscriptionNotInitializedException(final String subscriptionId) {
        super("Subscription " + subscriptionId + " is not initialized");
    }
}
