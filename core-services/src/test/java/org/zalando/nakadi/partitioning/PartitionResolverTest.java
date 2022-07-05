package org.zalando.nakadi.partitioning;

import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;

public class PartitionResolverTest {

    private PartitionResolver partitionResolver;
    private List<String> partitions = List.of("0");

    @Before
    public void before() {
        partitionResolver = new PartitionResolver(mock(HashPartitionStrategy.class));
    }

    @Test
    public void whenGetWithKnownStrategyThenOk() {
        final EventType eventType = new EventType();
        eventType.setPartitionStrategy(RANDOM_STRATEGY);

        final PartitionStrategy strategy = partitionResolver.getPartitionStrategy(eventType);
        assertThat(strategy, notNullValue());
    }

    @Test(expected = NoSuchPartitionStrategyException.class)
    public void whenGetWithUnknownStrategyThenException() {
        final EventType eventType = new EventType();
        eventType.setPartitionStrategy("blah_strategy");

        partitionResolver.getPartitionStrategy(eventType);
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
