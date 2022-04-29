package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.atLeastOnce;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

@RunWith(MockitoJUnitRunner.class)
public class PartitionResolverTest {

    private PartitionResolver partitionResolver;
    private TimelineService timelineService;
    private HashPartitionStrategy hashPartitionStrategy;
    @Captor
    private ArgumentCaptor<PartitioningData> partitioningDataCaptor;

    @Before
    public void before() {
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);
        when(topicRepository.listPartitionNames(any(String.class))).thenReturn(ImmutableList.of("0"));
        timelineService = Mockito.mock(TimelineService.class);
        when(timelineService.getTopicRepository((EventType) any())).thenReturn(topicRepository);
        hashPartitionStrategy = mock(HashPartitionStrategy.class);
        partitionResolver = new PartitionResolver(timelineService, hashPartitionStrategy);
    }

    @Test
    public void whenResolvePartitionWithKnownStrategyThenOk() {

        final EventType eventType = new EventType();
        eventType.setPartitionStrategy(RANDOM_STRATEGY);

        final Timeline mockTimeline = mock(Timeline.class);
        when(mockTimeline.getTopic()).thenReturn("topic-id");
        when(timelineService.getActiveTimeline(eq(eventType))).thenReturn(mockTimeline);

        final JSONObject event = new JSONObject();
        event.put("abc", "blah");

        final String partition = partitionResolver.resolvePartition(eventType, event);
        assertThat(partition, notNullValue());
    }

    @Test
    public void whenResolvePartitionForJsonEvents() {

        final EventType eventType = new EventType();
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final Timeline mockTimeline = mock(Timeline.class);
        when(mockTimeline.getTopic()).thenReturn("topic-id");
        when(timelineService.getActiveTimeline(eq(eventType))).thenReturn(mockTimeline);

        final JSONObject event = new JSONObject();

        final PartitioningData partitioningData = new PartitioningData();

        when(hashPartitionStrategy.getDataFromJson(eventType, event)).thenReturn(partitioningData);
        when(hashPartitionStrategy.calculatePartition(eq(partitioningData), any())).thenReturn("0");

        final String partition = partitionResolver.resolvePartition(eventType, event);
        assertEquals("0", partition);
    }

    @Test
    public void whenResolvePartitionForMetadataObjects() {

        final EventType eventType = new EventType();
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final Timeline mockTimeline = mock(Timeline.class);
        when(mockTimeline.getTopic()).thenReturn("topic-id");
        when(timelineService.getActiveTimeline(eq(eventType))).thenReturn(mockTimeline);

        final NakadiMetadata metadata = mock(NakadiMetadata.class);
        when(metadata.getPartitionKeys()).thenReturn(List.of("abc"));
        when(hashPartitionStrategy.calculatePartition(any(), any())).thenReturn("0");

        final String partition = partitionResolver.resolvePartition(eventType, metadata);

        assertEquals("0", partition);
        verify(hashPartitionStrategy, atLeastOnce()).calculatePartition(partitioningDataCaptor.capture(), any());
        assertEquals(partitioningDataCaptor.getValue().getPartitionKeys(), List.of("abc"));
    }

    @Test(expected = PartitioningException.class)
    public void whenResolvePartitionWithUnknownStrategyThenPartitioningException() {
        final EventType eventType = new EventType();
        eventType.setPartitionStrategy("blah_strategy");

        partitionResolver.resolvePartition(eventType, new JSONObject());
    }

    @Test(expected = NoSuchPartitionStrategyException.class)
    public void whenValidateWithUnknownPartitionStrategyThenExceptionThrown() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setPartitionStrategy("unknown_strategy");

        partitionResolver.validate(eventType);
    }

    @Test(expected = NoSuchPartitionStrategyException.class)
    public void whenValidateWithNullPartitionStrategyNameThenExceptionThrown() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setPartitionStrategy(null);

        partitionResolver.validate(eventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void whenValidateWithHashPartitionStrategyAndWithoutPartitionKeysThenExceptionThrown() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setPartitionStrategy(HASH_STRATEGY);

        partitionResolver.validate(eventType);
    }

    @Test(expected = InvalidEventTypeException.class)
    public void whenValidateWithUserDefinedPartitionStrategyForUndefinedCategoryThenExceptionThrown() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setCategory(UNDEFINED);
        eventType.setPartitionStrategy(USER_DEFINED_STRATEGY);

        partitionResolver.validate(eventType);
    }

    @Test
    public void whenValidateWithKnownPartitionStrategyThenOk() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setPartitionStrategy(RANDOM_STRATEGY);

        partitionResolver.validate(eventType);
    }
}
