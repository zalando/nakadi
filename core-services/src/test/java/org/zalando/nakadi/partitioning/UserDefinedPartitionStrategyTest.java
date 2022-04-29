package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.json.JSONObject;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.PartitioningException;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.zalando.nakadi.partitioning.PartitionStrategy.USER_DEFINED_STRATEGY;

public class UserDefinedPartitionStrategyTest {

    private static final UserDefinedPartitionStrategy STRATEGY = new UserDefinedPartitionStrategy();
    private static final List<String> PARTITIONS = ImmutableList.of("a", "b", "c");
    private final EventType eventType;

    public UserDefinedPartitionStrategyTest() {
        eventType = new EventType();
        eventType.setPartitionStrategy(USER_DEFINED_STRATEGY);
    }

    @Test
    public void whenCorrectPartitionThenOk() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"b\"}}");
        final String partition = STRATEGY.calculatePartition(STRATEGY.getDataFromJson(null, event), PARTITIONS);
        assertThat(partition, equalTo("b"));
    }

    @Test(expected = PartitioningException.class)
    public void whenIncorrectJsonThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition_id\":\"b\"}}");
        STRATEGY.calculatePartition(STRATEGY.getDataFromJson(null, event), PARTITIONS);
    }

    @Test(expected = PartitioningException.class)
    public void whenUnknownPartitionThenPartitioningException() throws PartitioningException {
        final JSONObject event = new JSONObject("{\"metadata\":{\"partition\":\"z\"}}");
        STRATEGY.calculatePartition(STRATEGY.getDataFromJson(null, event), PARTITIONS);
    }

}
