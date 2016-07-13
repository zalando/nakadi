package de.zalando.aruha.nakadi.service.subscription;

public interface SubscriptionStreamer {

    void stream() throws InterruptedException;
}
