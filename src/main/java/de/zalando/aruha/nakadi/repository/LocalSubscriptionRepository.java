package de.zalando.aruha.nakadi.repository;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import de.zalando.aruha.nakadi.domain.Subscription;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * This is just a local dummy storage until we decide on real one
 */
public class LocalSubscriptionRepository implements SubscriptionRepository {

    private Map<String, Subscription> subscriptions = Collections.synchronizedMap(Maps.newHashMap());

    private Map<String, List<Multimap<String, String>>> newDistributions = Collections.synchronizedMap(Maps.newHashMap());

    public LocalSubscriptionRepository() {
        // Database bootstrap :-D
        final Subscription sub1 = new Subscription("sub1", "test-topic");
        IntStream
                .range(0, 8)
                .boxed()
                .forEach(partition -> sub1.updateCursor(Integer.toString(partition), "0"));
        subscriptions.put(sub1.getSubscriptionId(), sub1);
    }

    @Override
    public Subscription getSubscription(final String subscriptionId) {
        return subscriptions.get(subscriptionId);
    }

    @Override
    public void saveSubscription(final Subscription subscription) {
        subscriptions.put(subscription.getSubscriptionId(), subscription);
    }

    @Override
    public String generateNewClientId(final Subscription subscription) {
        return UUID.randomUUID().toString();
    }

    @Override
    public void launchNewPartitionDistribution(final String subscriptionId, final Multimap<String, String> newDistribution) {
        List<Multimap<String, String>> newDistributionsQueue = newDistributions.get(subscriptionId);
        if (newDistributionsQueue == null) {
            newDistributionsQueue = Collections.synchronizedList(Lists.newArrayList());
            newDistributions.put(subscriptionId, newDistributionsQueue);
        }
        newDistributionsQueue.add(Multimaps.synchronizedMultimap(newDistribution));
    }

    @Override
    public Optional<Multimap<String, String>> checkForNewPartitionDistribution(final String subscriptionId) {
        final List<Multimap<String, String>> newDistributionsQueue = newDistributions.get(subscriptionId);
        if (newDistributionsQueue != null && newDistributionsQueue.size() > 0) {
            return Optional.of(newDistributionsQueue.get(0));
        }
        else {
            return Optional.empty();
        }
    }

    @Override
    public void clearProcessedRedistribution(final String subscriptionId, final List<String> clientIds) {
        final List<Multimap<String, String>> redistributionQueue = newDistributions.get(subscriptionId);
        if (redistributionQueue != null && redistributionQueue.size() > 0) {
            final Multimap<String, String> currentRedistribution = redistributionQueue.get(0);
            clientIds.stream().forEach(currentRedistribution::removeAll);
            if (currentRedistribution.keySet().size() == 0) {
                redistributionQueue.remove(0);
            }
        }
    }
}
