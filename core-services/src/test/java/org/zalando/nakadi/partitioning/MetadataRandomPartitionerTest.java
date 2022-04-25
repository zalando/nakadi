package org.zalando.nakadi.partitioning;

import org.junit.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;
import java.util.Random;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

public class MetadataRandomPartitionerTest {
    private final EventTypeCache eventTypeCache = Mockito.mock(EventTypeCache.class);
    private final TimelineService timelineService = Mockito.mock(TimelineService.class);

    @Test
    public void testItGeneratesPartitionsRandomly() {
        final var topicRepositoryMock = Mockito.mock(TopicRepository.class);
        final var timelineMock = Mockito.mock(Timeline.class);
        final var eventTypeName = "some-event-type";
        final var random = Mockito.mock(Random.class);

        when(eventTypeCache.getEventType(eventTypeName)).thenReturn(new EventType());
        when(timelineService.getTopicRepository(ArgumentMatchers.any(EventType.class))).thenReturn(topicRepositoryMock);
        when(timelineMock.getTopic()).thenReturn("some-topic-id");
        when(timelineService.getActiveTimeline(ArgumentMatchers.any(EventType.class))).thenReturn(timelineMock);
        when(topicRepositoryMock.listPartitionNames(ArgumentMatchers.anyString())).thenReturn(List.of("1", "2", "3"));
        when(random.nextInt(3)).thenReturn(1);

        final var metadataRandomPartitioner = new MetadataRandomPartitioner(eventTypeCache, timelineService, random);

        assertEquals(2, metadataRandomPartitioner.calculatePartition(eventTypeName));
    }
}
