package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.PartitionStrategyDescriptor;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import static de.zalando.aruha.nakadi.service.StrategiesRegistry.HASH_PARTITION_STRATEGY;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Matchers.any;

public class PartitionResolverTest {

    private PartitionResolver partitionResolver;

    @Before
    public void before() throws NakadiException {
        final TopicRepository topicRepository = Mockito.mock(TopicRepository.class);
        Mockito.when(topicRepository.listPartitionNames(any(String.class))).thenReturn(ImmutableList.of("0"));
        partitionResolver = new PartitionResolver(topicRepository);
    }

    @Test
    public void whenResolvePartitionWithKnownStrategyThenOk() throws NakadiException {

        final EventType eventType = new EventType();
        eventType.setPartitionKeyFields(ImmutableList.of("abc"));
        eventType.setPartitionStrategy(HASH_PARTITION_STRATEGY);

        final JSONObject event = new JSONObject();
        event.put("abc", "blah");

        final String partition = partitionResolver.resolvePartition(eventType, event);
        assertThat(partition, notNullValue());
    }

    @Test(expected = PartitioningException.class)
    public void whenResolvePartitionWithUnknownStrategyThenPartitioningException() throws NakadiException {
        final EventType eventType = new EventType();
        final PartitionStrategyDescriptor strategy = new PartitionStrategyDescriptor("blah_strategy");
        eventType.setPartitionStrategy(strategy);

        partitionResolver.resolvePartition(eventType, null);
    }
}
