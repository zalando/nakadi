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
        final List<String> partitions = ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h");

        final Random randomMock = mock(Random.class);
        when(randomMock.nextInt(anyInt())).thenReturn(3, 0, 4, 1, 7);
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy(randomMock);

        final List<String> resolvedPartitions = newArrayList();
        final int numberOfRuns = 5;
        for (int run = 0; run < numberOfRuns; run++) {
            final String resolvedPartition = strategy.calculatePartition(partitions);
            resolvedPartitions.add(resolvedPartition);
        }

        assertThat(resolvedPartitions, equalTo(ImmutableList.of("d", "a", "e", "b", "h")));
        verify(randomMock, times(5)).nextInt(partitions.size());
    }

    @Test
    public void whenOnePartitionThenRandomGeneratorNotUsed() {
        final Random randomMock = mock(Random.class);
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy(randomMock);

        final String resolvedPartition = strategy.calculatePartition(ImmutableList.of("a"));
        assertThat(resolvedPartition, equalTo("a"));

        verify(randomMock, Mockito.never()).nextInt(anyInt());
    }

    @Test
    public void calculatesPartitionsForJsonAndAvro() {
        final Random randomMock = mock(Random.class);
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy(randomMock);

        strategy.calculatePartition(null, null, ImmutableList.of("a", "b"));
        strategy.calculatePartition(null, ImmutableList.of("a", "b", "c"));

        verify(randomMock, Mockito.times(1)).nextInt(2);
        verify(randomMock, Mockito.times(1)).nextInt(3);
    }
}
