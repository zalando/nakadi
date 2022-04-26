package org.zalando.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.List;
import java.util.Random;

import static com.google.common.collect.Lists.newArrayList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class RandomPartitionStrategyTest {

    @Test
    public void whenCalculatePartitionThenRandomGeneratorIsUsedCorrectly() {
        final int partitions = 8;

        final Random randomMock = mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(3, 0, 4, 1, 7);
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy(randomMock);

        final List<Integer> resolvedPartitions = newArrayList();
        final int numberOfRuns = 5;
        for (int run = 0; run < numberOfRuns; run++) {
            final int resolvedPartition = strategy.calculatePartition(null, null, partitions);
            resolvedPartitions.add(resolvedPartition);
        }

        assertThat(resolvedPartitions, equalTo(ImmutableList.of(3, 0, 4, 1, 7)));
        verify(randomMock, times(5)).nextInt(partitions);
    }

    @Test
    public void whenOnePartitionThenRandomGeneratorNotUsed() {
        final Random randomMock = mock(Random.class);
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy(randomMock);

        final int resolvedPartition = strategy.calculatePartition(null, null, 1);
        assertThat(resolvedPartition, equalTo(0));

        verify(randomMock, Mockito.never()).nextInt(anyInt());
    }

}
