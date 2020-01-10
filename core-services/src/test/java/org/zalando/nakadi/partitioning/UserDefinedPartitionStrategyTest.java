package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;
import org.zalando.nakadi.partitioning.UserDefinedPartitionStrategy;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UserDefinedPartitionStrategyTest {

    private static final UserDefinedPartitionStrategy STRATEGY = new UserDefinedPartitionStrategy();
    private static final List<String> PARTITIONS = ImmutableList.of("a", "b", "c");

    @Test
    public void whenCorrectPartitionThenOk() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"b\"}}");
        final String partition = STRATEGY.calculatePartition(null, event, PARTITIONS);
        assertThat(partition, equalTo("b"));
    }

    @Test(expected = PartitioningException.class)
    public void whenIncorrectJsonThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition_id\":\"b\"}}");
        STRATEGY.calculatePartition(null, event, PARTITIONS);
    }

    @Test(expected = PartitioningException.class)
    public void whenUnknownPartitionThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"z\"}}");
        STRATEGY.calculatePartition(null, event, PARTITIONS);
    }

}
