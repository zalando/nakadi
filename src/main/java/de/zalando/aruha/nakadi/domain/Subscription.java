package de.zalando.aruha.nakadi.domain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;

/**
 * This is just a dummy that will be replaced when we finalize the high level consuming architecture
 */
public class Subscription {

    private String subscriptionId;
    private String topic;
    private List<String> clientIds;
    private Map<String, String> cursors;

    public Subscription(final String subscriptionId, final String topic) {
        this.subscriptionId = subscriptionId;
        this.topic = topic;
        clientIds = Lists.newArrayList();
        cursors = Maps.newHashMap();
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getClientIds() {
        return ImmutableList.copyOf(clientIds);
    }

    public void addClient(final String newClientId) {
        clientIds.add(newClientId);
    }

    public void removeClient(final String clientId) {
        clientIds.remove(clientId);
    }

    public Map<String, String> getCursors() {
        return ImmutableMap.copyOf(cursors);
    }

    public void updateCursor(String partition, String offset) {
        cursors.put(partition, offset);
    }
}
