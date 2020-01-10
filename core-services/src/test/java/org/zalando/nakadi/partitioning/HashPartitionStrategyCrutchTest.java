package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.springframework.core.env.Environment;
import org.zalando.nakadi.partitioning.HashPartitionStrategyCrutch;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HashPartitionStrategyCrutchTest {

    @Test
    public void whenAdjustPartitionIndexThenOk() {
        final Environment env = mock(Environment.class);
        when(env.getProperty("nakadi.hashPartitioning.overrideOrder.p3", List.class))
                .thenReturn(ImmutableList.of("1", "2", "0"));
        when(env.getProperty("nakadi.hashPartitioning.overrideOrder.p4", List.class))
                .thenReturn(ImmutableList.of("2", "0", "1", "3"));
        when(env.getProperty("nakadi.hashPartitioning.overrideOrder.p5", List.class))
                .thenReturn(ImmutableList.of("4", "3", "0", "2", "1"));

        final HashPartitionStrategyCrutch hashStrategyCrutch = new HashPartitionStrategyCrutch(env, 4);

        // expect no adjustment as we haven't specified any order for 2-partitions case
        assertThat(hashStrategyCrutch.adjustPartitionIndex(1, 2),
                equalTo(1));

        // expect adjustment
        assertThat(hashStrategyCrutch.adjustPartitionIndex(1, 3),
                equalTo(2));

        // expect adjustment
        assertThat(hashStrategyCrutch.adjustPartitionIndex(2, 4),
                equalTo(1));

        // expect no adjustment as the maxPartitionNum we specified is 3,
        // so our predefined order for 5-partitions should be ignored
        assertThat(hashStrategyCrutch.adjustPartitionIndex(0, 5),
                equalTo(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenPartitionCountIsWrongThenIllegalArgumentException() {
        final Environment env = mock(Environment.class);
        when(env.getProperty("nakadi.hashPartitioning.overrideOrder.p3", List.class))
                .thenReturn(ImmutableList.of("1", "2", "0", "3"));
        new HashPartitionStrategyCrutch(env, 4);
    }

    @Test(expected = IllegalArgumentException.class)
    public void whenPartitionIndexIsWrongThenIllegalArgumentException() {
        final Environment env = mock(Environment.class);
        when(env.getProperty("nakadi.hashPartitioning.overrideOrder.p3", List.class))
                .thenReturn(ImmutableList.of("1", "0", "3"));
        new HashPartitionStrategyCrutch(env, 4);
    }

}
