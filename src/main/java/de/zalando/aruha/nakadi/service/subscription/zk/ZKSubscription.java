package de.zalando.aruha.nakadi.service.subscription.zk;

public interface ZKSubscription {
    void refresh();

    void cancel();
}
