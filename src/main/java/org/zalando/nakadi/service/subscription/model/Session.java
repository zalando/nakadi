package org.zalando.nakadi.service.subscription.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.zalando.nakadi.domain.EventTypePartition;

import java.util.List;
import java.util.UUID;

public class Session {

    private final String id;
    private final int weight;
    private final List<EventTypePartition> requestedPartitions;

    @JsonCreator
    public Session(@JsonProperty("id") final String id,
                   @JsonProperty("weight") final int weight,
                   @JsonProperty("requested_partitions") final List<EventTypePartition> requestedPartitions) {
        this.id = id;
        this.weight = weight;
        this.requestedPartitions = requestedPartitions;
    }

    public Session(final String id, final int weight) {
        this(id, weight, ImmutableList.of());
    }

    public String getId() {
        return id;
    }

    public int getWeight() {
        return weight;
    }

    public List<EventTypePartition> getRequestedPartitions() {
        return requestedPartitions;
    }

    @Override
    public String toString() {
        return "Session{" + id + ", weight=" + weight + ", requestedPartitions=" + requestedPartitions + "}";
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Session session = (Session) o;
        return weight == session.getWeight()
                && id.equals(session.getId())
                && requestedPartitions.equals(session.getRequestedPartitions());
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + weight;
        result = 31 * result + requestedPartitions.hashCode();
        return result;
    }

    public static Session generate(final int weight, final List<EventTypePartition> requestedPartitions) {
        return new Session(UUID.randomUUID().toString(), weight, requestedPartitions);
    }
}
