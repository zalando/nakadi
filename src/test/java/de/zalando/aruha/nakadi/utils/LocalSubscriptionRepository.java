package de.zalando.aruha.nakadi.utils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

/**
 * This is just a local dummy storage for tests
 */
public class LocalSubscriptionRepository implements SubscriptionRepository {

    private Map<String, Subscription> subscriptions = Collections.synchronizedMap(Maps.newHashMap());

    private Map<String, List<String>> topologies = Collections.synchronizedMap(Maps.newHashMap());

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

    public static class Subscription {

        private String subscriptionId;

        /**
         * topics to consume from
         */
        private List<String> topics;

        /**
         * this keeps the committed offsets
         */
        private List<Cursor> cursors;

        public Subscription(final String subscriptionId, final List<String> topics) {
            this.subscriptionId = subscriptionId;
            this.topics = ImmutableList.copyOf(topics);
            cursors = Lists.newArrayList();
        }

        public String getSubscriptionId() {
            return subscriptionId;
        }

        public List<String> getTopics() {
            return topics;
        }

        public List<Cursor> getCursors() {
            return Collections.unmodifiableList(cursors);
        }

        public Optional<Cursor> getCursor(final TopicPartition tp) {
            return cursors
                    .stream()
                    .filter(cursor ->
                            cursor.getTopic().equals(tp.getTopic()) && cursor.getPartition().equals(tp.getPartition()))
                    .findFirst();
        }

        public void updateCursor(String topic, String partition, String offset) {
            cursors
                    .stream()
                    .filter(cursor -> cursor.getPartition().equals(partition) && cursor.getTopic().equals(topic))
                    .findFirst()
                    .orElseGet(() -> {
                        final Cursor newCursor = new Cursor(topic, partition, offset);
                        cursors.add(newCursor);
                        return newCursor;
                    })
                    .setOffset(offset);
        }

        @Override
        public String toString() {
            return "Subscription{" +
                    "subscriptionId='" + subscriptionId + '\'' +
                    ", topics=" + topics +
                    ", cursors=" + cursors +
                    '}';
        }
    }
}



