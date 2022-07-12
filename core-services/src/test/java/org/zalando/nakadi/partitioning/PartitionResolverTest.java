package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;
import org.zalando.nakadi.exceptions.runtime.NoSuchPartitionStrategyException;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.domain.EventCategory.UNDEFINED;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.RANDOM_STRATEGY;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.loadEventType;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class PartitionResolverTest {

    private PartitionResolver partitionResolver;
    private List<String> partitions = List.of("0");

    @Before
    public void before() {
        partitionResolver = new PartitionResolver(mock(HashPartitionStrategy.class));
    }

    @Test
    public void whenResolvePartitionWithKnownStrategyThenOk() {

        final EventType eventType = new EventType();
        eventType.setPartitionStrategy(RANDOM_STRATEGY);

        final JSONObject event = new JSONObject();
        event.put("abc", "blah");

        final BatchItem item = mock(BatchItem.class);
        when(item.getEvent()).thenReturn(event);

        final String partition = partitionResolver.resolvePartition(eventType, item, partitions);
        assertThat(partition, notNullValue());
    }

    @Test(expected = PartitioningException.class)
    public void whenResolvePartitionWithUnknownStrategyThenPartitioningException() {
        final EventType eventType = new EventType();
        eventType.setPartitionStrategy("blah_strategy");

        partitionResolver.resolvePartition(eventType, mock(BatchItem.class), partitions);
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

    @Test
    public void whenValidateWithHashPartitionStrategyAndDataChangeEventLookupIntoDataField() throws Exception {
        final EventType eventType = loadEventType(
                "org/zalando/nakadi/domain/event-type.with.partition-key-fields.json");
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final List<String> partitionKeyFields = PartitionResolver.getKeyFieldsForExtraction(eventType);
        final JSONObject event = new JSONObject(readFile("sample-data-event.json"));

        assertThat(PartitionResolver.extractPartitionKeys(partitionKeyFields, event),
                equalTo(List.of("A1", "Super Shirt")));
    }

    @Test(expected = InvalidPartitionKeyFieldsException.class)
    public void whenPayloadIsMissingPartitionKeysThenItThrows() {
        PartitionResolver.extractPartitionKeys(List.of("body.sku"), new JSONObject());
    }
}
