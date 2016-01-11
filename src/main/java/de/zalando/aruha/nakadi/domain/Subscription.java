package de.zalando.aruha.nakadi.domain;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 * This is just a dummy that will be replaced when we finalize the high level consuming architecture
 */
public class Subscription {

    private String subscriptionId;
    private String topic;
    private List<String> clientIds;
    private List<Cursor> cursors;

    public Subscription(final String subscriptionId, final String topic) {
        this.subscriptionId = subscriptionId;
        this.topic = topic;
        clientIds = Lists.newArrayList();
        cursors = Lists.newArrayList();
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getClientIds() {
        return Collections.unmodifiableList(clientIds);
    }

    public void addClient(final String newClientId) {
        clientIds.add(newClientId);
    }

    public void removeClient(final String clientId) {
        clientIds.remove(clientId);
    }

    public List<Cursor> getCursors() {
        return Collections.unmodifiableList(cursors);
    }

    public void updateCursor(String partition, String offset) {
        cursors
                .stream()
                .filter(cursor -> cursor.getPartition().equals(partition))
                .findFirst()
                .orElseGet(() -> {
                    final Cursor newCursor = new Cursor(topic, partition, offset);
                    cursors.add(newCursor);
                    return newCursor;
                })
                .setOffset(offset);
    }
}
