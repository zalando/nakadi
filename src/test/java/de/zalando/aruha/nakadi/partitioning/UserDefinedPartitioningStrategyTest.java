package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.exceptions.PartitioningException;
import org.json.JSONObject;
import org.junit.Test;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UserDefinedPartitioningStrategyTest {

    private static final UserDefinedPartitioningStrategy strategy = new UserDefinedPartitioningStrategy();
    private static final List<String> partitions = ImmutableList.of("a", "b", "c");

    @Test
    public void whenCorrectPartitionThenOk() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"b\"}}");
        final String partition = strategy.calculatePartition(null, event, partitions);
        assertThat(partition, equalTo("b"));
    }

    @Test(expected = PartitioningException.class)
    public void whenIncorrectJsonThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition_id\":\"b\"}}");
        strategy.calculatePartition(null, event, partitions);
    }

    @Test(expected = PartitioningException.class)
    public void whenUnknownPartitionThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"z\"}}");
        strategy.calculatePartition(null, event, partitions);
    }

}
