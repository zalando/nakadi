package de.zalando.aruha.nakadi.repository;

import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topology;

import java.util.List;

public interface SubscriptionRepository {

    void createSubscription(String subscriptionId, List<String> topics, List<Cursor> cursors);

    List<String> getSubscriptionTopics(String subscriptionId);

    Cursor getCursor(String subscriptionId, String topic, String partition);

    void saveCursor(String subscriptionId, Cursor cursor);

    String generateNewClientId();

    void addClient(String subcriptionId, String clientId);

    void removeClient(String subcriptionId, String clientId);

    Topology getTopology(String subscriptionId);
}
