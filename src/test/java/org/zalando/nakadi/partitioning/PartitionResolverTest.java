package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.PartitioningException;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;
import static org.mockito.Matchers.any;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.loadEventType;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class PartitionResolverTest {

    private PartitionResolver partitionResolver;

    @Before
    public void before() throws NakadiException {
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);
        Mockito.when(topicRepository.listPartitionNames(any(String.class))).thenReturn(ImmutableList.of("0"));
        final TimelineService timelineService = Mockito.mock(TimelineService.class);
        Mockito.when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepository);
        Mockito.when(timelineService.getTopicRepository((EventType) any())).thenReturn(topicRepository);
        partitionResolver = new PartitionResolver(timelineService);
    }

    @Test
    public void whenResolvePartitionWithKnownStrategyThenOk() throws NakadiException {

        final EventType eventType = new EventType();
        eventType.setPartitionKeyFields(ImmutableList.of("abc"));
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final JSONObject event = new JSONObject();
        event.put("abc", "blah");

        final String partition = partitionResolver.resolvePartition(eventType, event);
        assertThat(partition, notNullValue());
    }

    @Test(expected = PartitioningException.class)
    public void whenResolvePartitionWithUnknownStrategyThenPartitioningException() throws NakadiException {
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

    @Test
    public void whenValidateWithHashPartitionStrategyAndDataChangeEventLookupIntoDataField() throws Exception {
        final EventType eventType = loadEventType(
                "org/zalando/nakadi/domain/event-type.with.partition-key-fields.json");
        eventType.setPartitionStrategy(HASH_STRATEGY);
        final JSONObject event = new JSONObject(readFile("sample-data-event.json"));

        assertThat(partitionResolver.resolvePartition(eventType, event), is(notNullValue()));
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
