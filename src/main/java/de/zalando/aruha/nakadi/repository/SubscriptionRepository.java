package de.zalando.aruha.nakadi.repository;

import com.google.common.collect.Multimap;
import de.zalando.aruha.nakadi.domain.Subscription;

import java.util.List;
import java.util.Optional;

public interface SubscriptionRepository {

    Subscription getSubscription(String subscriptionId);

    void saveSubscription(Subscription subscription);

    String generateNewClientId(Subscription subscription);

    void launchNewPartitionDistribution(String subscriptionId, Multimap<String, String> newDistribution);

    Optional<Multimap<String, String>> checkForNewPartitionDistribution(String subscriptionId);

    void clearProcessedRedistribution(String subscriptionId, List<String> clientIds);
}
