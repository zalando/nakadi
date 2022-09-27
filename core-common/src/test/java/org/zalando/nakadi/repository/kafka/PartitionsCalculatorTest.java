package org.zalando.nakadi.repository.kafka;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.zalando.nakadi.domain.EventTypeStatistics;

import java.util.stream.Stream;

import static org.junit.jupiter.params.provider.Arguments.arguments;

public class PartitionsCalculatorTest {

    private static final int MAX_PARTITION_COUNT = 1000;

    private static PartitionsCalculator buildTest() {
        return new PartitionsCalculator(1, MAX_PARTITION_COUNT);
    }

    @ParameterizedTest
    @MethodSource("partitionTestCases")
    public void test(final int readParallelism,
                     final int writeParallelism,
                     final int expectedBestPartitionCount) {
        final PartitionsCalculator calculator = buildTest();
        final EventTypeStatistics statistics = new EventTypeStatistics();
        statistics.setReadParallelism(readParallelism);
        statistics.setWriteParallelism(writeParallelism);
        Assertions.assertEquals(calculator.getBestPartitionsCount(statistics), expectedBestPartitionCount);
    }

    static Stream<Arguments> partitionTestCases() {
        // readParallelism, writeParallelism, expectedBestPartitionCount
        return Stream.of(
                arguments(0, -1, 1),
                arguments(-1, 0, 1),
                arguments(-1, -1, 1),
                arguments(0, 0, 1),
                arguments(2, 2, 2),
                arguments(2, 3, 3),
                arguments(3, 2, 3),
                arguments(2000, 100, MAX_PARTITION_COUNT)
        );
    }

}
