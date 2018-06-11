package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeBaseException;

public class SubscriptionNotInitializedException extends NakadiRuntimeBaseException {

    public SubscriptionNotInitializedException(final String subscriptionId) {
        super("Subscription " + subscriptionId + " is not initialized");
    }
}
