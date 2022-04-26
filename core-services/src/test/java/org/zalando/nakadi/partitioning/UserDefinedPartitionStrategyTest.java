package org.zalando.nakadi.partitioning;

import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class UserDefinedPartitionStrategyTest {

    private static final UserDefinedPartitionStrategy STRATEGY = new UserDefinedPartitionStrategy();
    private static final int PARTITIONS = 3;

    @Test
    public void whenCorrectPartitionThenOk() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"1\"}}");
        final int partition = STRATEGY.calculatePartition(null, event, PARTITIONS);
        assertThat(partition, equalTo(1));
    }

    @Test(expected = PartitioningException.class)
    public void whenIncorrectJsonThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition_id\":\"b\"}}");
        STRATEGY.calculatePartition(null, event, PARTITIONS);
    }

    @Test(expected = PartitioningException.class)
    public void whenUnknownPartitionThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"5\"}}");
        STRATEGY.calculatePartition(null, event, PARTITIONS);
    }

}
