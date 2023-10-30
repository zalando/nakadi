package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.InvalidPartitionKeyFieldsException;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.partitioning.PartitionStrategy.HASH_STRATEGY;
import static org.zalando.nakadi.utils.TestUtils.loadEventType;
import static org.zalando.nakadi.utils.TestUtils.readFile;

public class EventKeyExtractorTest {
    @Test
    public void whenExtractKeysThenCorrectValuesExtracted() throws Exception {
        final EventType eventType = loadEventType(
                "org/zalando/nakadi/domain/event-type.with.partition-key-fields.json");
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final BatchItem item = mock(BatchItem.class);
        when(item.getEvent()).thenReturn(new JSONObject(readFile("sample-data-event.json")));

        final Function<BatchItem, List<String>> extractor = EventKeyExtractor.partitionKeysFromBatchItem(eventType);
        assertThat(extractor.apply(item), equalTo(List.of("A1", "Super Shirt")));
    }

    @Test(expected = InvalidPartitionKeyFieldsException.class)
    public void whenPayloadIsMissingPartitionKeysThenItThrows() throws IOException {
        final EventType eventType = loadEventType(
                "org/zalando/nakadi/domain/event-type.with.partition-key-fields.json");
        eventType.setPartitionStrategy(HASH_STRATEGY);

        final BatchItem item = mock(BatchItem.class);
        when(item.getEvent()).thenReturn(new JSONObject());

        final Function<BatchItem, List<String>> extractor = EventKeyExtractor.partitionKeysFromBatchItem(eventType);
        extractor.apply(item);
    }

}