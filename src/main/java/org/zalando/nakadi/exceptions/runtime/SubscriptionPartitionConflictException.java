package org.zalando.nakadi.exceptions.runtime;

import org.zalando.nakadi.domain.EventTypePartition;

import java.util.List;
import java.util.stream.Collectors;

public class SubscriptionPartitionConflictException extends NakadiBaseException {

    private SubscriptionPartitionConflictException(final String msg) {
        super(msg);
    }

    public static SubscriptionPartitionConflictException of(final List<EventTypePartition> conflictPartitions) {
        final String partitionsDesc = conflictPartitions.stream()
                .map(EventTypePartition::toString)
                .collect(Collectors.joining(","));

        return new SubscriptionPartitionConflictException("The following partitions are already requested by other " +
                "stream(s) of this subscription: " + partitionsDesc);
    }

}
