package org.zalando.nakadi.domain;

import java.util.Collections;
import java.util.Set;

import javax.annotation.concurrent.Immutable;

import com.fasterxml.jackson.annotation.JsonInclude;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Immutable
@AllArgsConstructor
public class SubscriptionEventTypeStats {

    private final String eventType;
    private final Set<Partition> partitions;

    public String getEventType() {
        return eventType;
    }

    public Set<Partition> getPartitions() {
        return Collections.unmodifiableSet(partitions);
    }

    @Immutable
    @Getter
    @AllArgsConstructor
    public static class Partition {
        private final String partition;
        private final String state;
        @JsonInclude(JsonInclude.Include.NON_NULL)
        private final Long unconsumedEvents;
        private final String streamId;
    }
}
