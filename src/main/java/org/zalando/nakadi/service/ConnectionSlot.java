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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final ConnectionSlot that = (ConnectionSlot) o;
        return client.equals(that.client)
                && eventType.equals(that.eventType)
                && partition.equals(that.partition)
                && connectionId.equals(that.connectionId);
    }

    @Override
    public int hashCode() {
        int result = client.hashCode();
        result = 31 * result + eventType.hashCode();
        result = 31 * result + partition.hashCode();
        result = 31 * result + connectionId.hashCode();
        return result;
    }
}
