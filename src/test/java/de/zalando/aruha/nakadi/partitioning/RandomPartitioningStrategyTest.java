package de.zalando.aruha.nakadi.partitioning;

import com.google.common.collect.ImmutableList;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

public class RandomPartitioningStrategyTest {

    private static final RandomPartitioningStrategy strategy = new RandomPartitioningStrategy();

    @Test
    public void whenManyRunsThenAllPartitionsAreUsed() {
        final List<String> partitions = ImmutableList.of("a", "b", "c", "d", "e", "f", "g", "h");
        final Set<String> resolvedPartitions = newHashSet();

        final int numberOfRuns = 1000;
        for (int run = 0; run < numberOfRuns; run++) {
            final String resolvedPartition = strategy.calculatePartition(null, null, partitions);
            resolvedPartitions.add(resolvedPartition);
        }

        assertThat(resolvedPartitions, equalTo(newHashSet(partitions)));
    }

    @Test
    public void whenOnePartitionThenUseIt() {
        final String resolvedPartition = strategy.calculatePartition(null, null, ImmutableList.of("a"));
        assertThat(resolvedPartition, equalTo("a"));
    }

}
