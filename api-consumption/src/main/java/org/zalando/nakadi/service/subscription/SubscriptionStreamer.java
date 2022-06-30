package org.zalando.nakadi.service.subscription;

public interface SubscriptionStreamer {

    void stream() throws InterruptedException;

    void terminateStream();

}
