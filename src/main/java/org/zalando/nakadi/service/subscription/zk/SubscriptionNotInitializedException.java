package org.zalando.nakadi.service.subscription.zk;

import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;

public class SubscriptionNotInitializedException extends MyNakadiRuntimeException1 {

    public SubscriptionNotInitializedException(final String subscriptionId) {
        super("Subscription " + subscriptionId + " is not initialized");
    }
}
