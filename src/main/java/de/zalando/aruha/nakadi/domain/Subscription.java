package de.zalando.aruha.nakadi.domain;

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

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public String getTopic() {
        return topic;
    }

    public List<String> getClientIds() {
        return clientIds;
    }

    public Map<String, String> getCursors() {
        return cursors;
    }
}
