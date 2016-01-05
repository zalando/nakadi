package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.Subscription;

public interface SubscriptionRepository {

    Subscription getSubscription(String subscriptionId);

}
