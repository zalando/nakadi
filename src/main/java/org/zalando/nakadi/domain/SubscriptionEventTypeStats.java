package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.Set;

@Immutable
public class SubscriptionEventTypeStats {

    private final String eventType;
    private final Set<Partition> partitions;

    public SubscriptionEventTypeStats(final String eventType, final Set<Partition> partitions) {
        this.eventType = eventType;
        this.partitions = partitions;
    }

    public String getEventType() {
        return eventType;
    }

    public Set<Partition> getPartitions() {
        return Collections.unmodifiableSet(partitions);
    }

    @Immutable
    public static class Partition {
        private final String partition;
        private final String state;
        private final long unconsumedEvents;
        private final String clientId;

        public Partition(final String partition,
                          final String state,
                          final long unconsumedEvents,
                          final String clientId) {
            this.partition = partition;
            this.state = state;
            this.unconsumedEvents = unconsumedEvents;
            this.clientId = clientId;
        }

        public String getPartition() {
            return partition;
        }

        public String getState() {
            return state;
        }

        public long getUnconsumedEvents() {
            return unconsumedEvents;
        }

        public String getClientId() {
            return clientId;
        }
    }
}
