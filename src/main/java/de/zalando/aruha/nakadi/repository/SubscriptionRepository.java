package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.domain.Subscription;

import java.util.Optional;

public interface SubscriptionRepository {

    Subscription getSubscription(String subscriptionId);

    void saveSubscription(Subscription subscription);

    String generateNewClientId(Subscription subscription);

    void setNewTopology(String subscriptionId, Topology newTopology);

    Optional<Topology> getTopology(String subscriptionId);
}
