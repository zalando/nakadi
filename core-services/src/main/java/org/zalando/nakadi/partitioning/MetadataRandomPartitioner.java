package org.zalando.nakadi.partitioning;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;
import java.util.Random;

@Component
public class MetadataRandomPartitioner {
    private final Random random;
    private final EventTypeCache eventTypeCache;
    private final TimelineService timelineService;

    @Autowired
    public MetadataRandomPartitioner(final EventTypeCache eventTypeCache,
                                     final TimelineService timelineService) {
        this.random = new Random();
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
    }

    public MetadataRandomPartitioner(final EventTypeCache eventTypeCache,
                                     final TimelineService timelineService,
                                     final Random random) {
        this.random = random;
        this.eventTypeCache = eventTypeCache;
        this.timelineService = timelineService;
    }

    public int calculatePartition(final String eventTypeName) {
        final var eventType = eventTypeCache.getEventType(eventTypeName);
        final List<String> partitions = timelineService.getTopicRepository(eventType)
                .listPartitionNames(timelineService.getActiveTimeline(eventType).getTopic());

        if (partitions.size() == 1) {
            return Integer.parseInt(partitions.get(0));
        } else {
            final int partitionIndex = random.nextInt(partitions.size());
            return Integer.parseInt(partitions.get(partitionIndex));
        }
    }
}
