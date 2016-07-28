package org.zalando.nakadi.service.subscription.zk;

public interface ZKSubscription {
    void refresh();

    void cancel();
}
