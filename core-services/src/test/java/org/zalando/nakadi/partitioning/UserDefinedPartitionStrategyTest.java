package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UserDefinedPartitionStrategyTest {

    private static final UserDefinedPartitionStrategy STRATEGY = new UserDefinedPartitionStrategy();
    private static final List<String> PARTITIONS = ImmutableList.of("0", "1", "2");

    @Test
    public void whenCorrectPartitionThenOk() {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"1\"}}");

        final BatchItem item = Mockito.mock(BatchItem.class);
        Mockito.when(item.getEvent()).thenReturn(event);
        assertThat(STRATEGY.calculatePartition(item, PARTITIONS), equalTo(PARTITIONS.get(1)));

        final var metadata = Mockito.mock(NakadiMetadata.class);
        Mockito.when(metadata.getPartition()).thenReturn("1");
        assertThat(STRATEGY.calculatePartition(metadata, PARTITIONS), equalTo(PARTITIONS.get(1)));
    }

    @Test(expected = PartitioningException.class)
    public void whenIncorrectPartitionInMetadataThenPartitioningException() throws PartitioningException {
        STRATEGY.resolvePartition("", PARTITIONS);
    }

    @Test(expected = PartitioningException.class)
    public void whenUnknownPartitionThenPartitioningException() throws PartitioningException {
        STRATEGY.resolvePartition("4", PARTITIONS);
    }
}
