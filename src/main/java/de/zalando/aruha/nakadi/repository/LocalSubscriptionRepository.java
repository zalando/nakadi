package de.zalando.aruha.nakadi.repository;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.domain.Subscription;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.IntStream;

/**
 * This is just a local dummy storage until we decide on real one
 */
public class LocalSubscriptionRepository implements SubscriptionRepository {

    private Map<String, Subscription> subscriptions = Collections.synchronizedMap(Maps.newHashMap());

    private Map<String, Topology> simpleTopologies = Collections.synchronizedMap(Maps.newHashMap());

    public LocalSubscriptionRepository() {
        // Database bootstrap :-D
        final Subscription sub1 = new Subscription("sub1", ImmutableList.of("test-topic"));
        IntStream
                .range(0, 8)
                .boxed()
                .forEach(partition -> sub1.updateCursor("test-topic", Integer.toString(partition), "0"));
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
    public void setNewTopology(final String subscriptionId, final Topology newTopology) {
        simpleTopologies.put(subscriptionId, newTopology);
    }

    @Override
    public Optional<Topology> getTopology(final String subscriptionId) {
        return Optional.ofNullable(simpleTopologies.get(subscriptionId));
    }
}
