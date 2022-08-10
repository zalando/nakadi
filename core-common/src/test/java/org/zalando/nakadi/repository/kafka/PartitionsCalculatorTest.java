package org.zalando.nakadi.repository.kafka;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.utils.TestUtils;

import java.io.IOException;
import java.io.InputStream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class PartitionsCalculatorTest {

    private static final int MAX_PARTITION_COUNT = 1000;

    private static PartitionsCalculator buildTest() throws IOException {
        return PartitionsCalculator.load(TestUtils.OBJECT_MAPPER, "m5.large", getTestStream(), 0, MAX_PARTITION_COUNT);
    }

    private static InputStream getTestStream() {
        return PartitionsCalculator.class.getResourceAsStream("test_partitions_statistics.json");
    }

    @Test(expected = IllegalArgumentException.class)
    public void testLoadFailForUnknownName() throws IOException {
        PartitionsCalculator.load(TestUtils.OBJECT_MAPPER, "null", getTestStream(), 0, 1000);
    }

    @Test
    public void testLoadCorrectForCorrectName() throws IOException {
        for (final String name : new String[]{"m5.large", "c4.xlarge"}) {
            final PartitionsCalculator calculatorMap = PartitionsCalculator.load(TestUtils.OBJECT_MAPPER, name,
                    getTestStream(), 0, MAX_PARTITION_COUNT);
            assertThat(calculatorMap, notNullValue());
        }
    }

    @Test
    public void ensureCorrectValuesReturnedForSimpleCase() throws IOException {
        final PartitionsCalculator calculator = buildTest();
        // 48.12, 74.83, 92.89, 84.69, 91.19, 94.22, 88.34, 86.35
        assertThat(calculator.getBestPartitionsCount(1000, 48.12f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(1000, 48.13f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(1000, 74.82f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(1000, 74.84f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(1000, 92.88f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(1000, 92.90f), equalTo(6));
        // ensure that we will try our best to perform task.
        assertThat(calculator.getBestPartitionsCount(1000, 100.f), equalTo(6));
    }

    @Test
    public void ensureCorrectValuesReturnedForCentralCase() throws IOException {
        final PartitionsCalculator calculator = buildTest();
        final int testCaseCount = 100;
        for (int i = 0; i < testCaseCount; ++i) {
            final float mbsPerSecond = (100.f * i) / testCaseCount;
            final int countLower = calculator.getBestPartitionsCount(100, mbsPerSecond);
            final int countUpper = calculator.getBestPartitionsCount(1000, mbsPerSecond);
            final int countBetween = calculator.getBestPartitionsCount(550, mbsPerSecond);
            if (countLower > countUpper) {
                assertThat(countBetween, lessThanOrEqualTo(countLower));
                assertThat(countBetween, greaterThanOrEqualTo(countUpper));
            } else {
                assertThat(countBetween, lessThanOrEqualTo(countUpper));
                assertThat(countBetween, greaterThanOrEqualTo(countLower));
            }
        }
    }

    @Test
    public void testSmallSizes() throws IOException {
        final PartitionsCalculator calculator = buildTest();
        // 25.27, 24.69, 25.48, 25.59, 24.95, 25.12, 25.17, 25.93
        assertThat(calculator.getBestPartitionsCount(0, 22.f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(0, 25.27f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(0, 25.28f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(0, 26.f), equalTo(8));
    }

    @Test
    public void testLargeSizes() throws IOException {
        final PartitionsCalculator calculator = buildTest();
        // 35.34, 55.52, 57.14, 76.96, 89.56, 97.54, 94.73, 96.69
        assertThat(calculator.getBestPartitionsCount(200000, 22.f), equalTo(1));
        assertThat(calculator.getBestPartitionsCount(200000, 35.36f), equalTo(2));
        assertThat(calculator.getBestPartitionsCount(200000, 57.f), equalTo(3));
        assertThat(calculator.getBestPartitionsCount(200000, 96.f), equalTo(6));
        assertThat(calculator.getBestPartitionsCount(200000, 100.f), equalTo(6));
    }

    @Test
    public void testIntegerOverflowOnStatisticsCalculation() throws IOException {
        final PartitionsCalculator calculator = buildTest();
        final EventTypeStatistics statistics = new EventTypeStatistics();
        statistics.setReadParallelism(1);
        statistics.setWriteParallelism(1);
        statistics.setMessagesPerMinute(1000000000);
        statistics.setMessageSize(1000000000);
        Assert.assertThat(calculator.getBestPartitionsCount(statistics), equalTo(6));
    }

}
