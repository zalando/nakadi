package org.zalando.nakadi.service;

import javax.annotation.concurrent.Immutable;

@Immutable
public class ConnectionSlot {

    private final String client;

    private final String eventType;

    private final String partition;

    private final String connectionId;

    public ConnectionSlot(final String client, final String eventType, final String partition,
                          final String connectionId) {
        this.client = client;
        this.eventType = eventType;
        this.partition = partition;
        this.connectionId = connectionId;
    }

    public String getClient() {
        return client;
    }

    public String getEventType() {
        return eventType;
    }

    public String getPartition() {
        return partition;
    }

    public String getConnectionId() {
        return connectionId;
    }
}
