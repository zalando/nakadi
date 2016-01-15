package de.zalando.aruha.nakadi.repository;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.domain.Topology;
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

    private Map<String, List<String>> topologies = Collections.synchronizedMap(Maps.newHashMap());

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
    public void createSubscription(final String subscriptionId, final List<String> topics, final List<Cursor> cursors) {
        final Subscription subscription = new Subscription(subscriptionId, topics);
        cursors.forEach(cursor -> subscription.updateCursor(cursor.getTopic(), cursor.getPartition(), cursor.getOffset()));
        subscriptions.put(subscriptionId, subscription);
    }

    @Override
    public List<String> getSubscriptionTopics(final String subscriptionId) {
        return subscriptions.get(subscriptionId).getTopics();
    }

    @Override
    public Cursor getCursor(final String subscriptionId, final String topic, final String partition) {
        return subscriptions.get(subscriptionId).getCursor(new TopicPartition(topic, partition)).get();
    }

    @Override
    public void saveCursor(final String subscriptionId, final Cursor cursor) {
        subscriptions.get(subscriptionId).updateCursor(cursor.getTopic(), cursor.getPartition(), cursor.getOffset());
    }

    @Override
    public String generateNewClientId() {
        return UUID.randomUUID().toString();
    }

    @Override
    public void addClient(final String subcriptionId, final String clientId) {
        Optional
                .ofNullable(topologies.get(subcriptionId))
                .orElseGet(() -> {
                    final List<String> clientIds = Collections.synchronizedList(Lists.newArrayList());
                    topologies.put(subcriptionId, clientIds);
                    return clientIds;
                })
                .add(clientId);
    }

    @Override
    public void removeClient(final String subcriptionId, final String clientId) {
        Optional
                .ofNullable(topologies.get(subcriptionId))
                .ifPresent(clientIds -> clientIds.remove(clientId));
    }

    @Override
    public Topology getTopology(final String subscriptionId) {
        final List<String> clientIds = Optional
                .ofNullable(topologies.get(subscriptionId))
                .orElse(Lists.newArrayList());
        return new Topology(clientIds);
    }
}
