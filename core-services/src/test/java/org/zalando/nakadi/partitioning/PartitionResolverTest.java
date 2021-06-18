package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class PartitionResolverTest {

    private PartitionResolver partitionResolver;
    private TimelineService timelineService;

    @Before
    public void before() {
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);
        when(topicRepository.listPartitionNames(any(String.class))).thenReturn(ImmutableList.of("0"));
        timelineService = Mockito.mock(TimelineService.class);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        when(timelineService.getTopicRepository((EventType) any())).thenReturn(topicRepository);
        partitionResolver = new PartitionResolver(timelineService, mock(HashPartitionStrategy.class));
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

    @Test(expected = PartitioningException.class)
    public void whenResolvePartitionWithUnknownStrategyThenPartitioningException() {
        final EventType eventType = new EventType();
        eventType.setPartitionStrategy("blah_strategy");

        partitionResolver.resolvePartition(eventType, null);
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
