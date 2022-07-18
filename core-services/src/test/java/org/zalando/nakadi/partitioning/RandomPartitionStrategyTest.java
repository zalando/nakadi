package org.zalando.nakadi.partitioning;

import org.junit.Test;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.isIn;

public class RandomPartitionStrategyTest {
    @Test
    public void selectsPartitionFromTheGivenList() {
        final RandomPartitionStrategy strategy = new RandomPartitionStrategy();

        final List<String> partitions = List.of("a", "b", "c");
        assertThat(strategy.getRandomPartition(partitions), isIn(partitions));
    }
}
