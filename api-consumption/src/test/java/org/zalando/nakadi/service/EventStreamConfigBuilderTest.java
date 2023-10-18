package org.zalando.nakadi.service;

import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.InvalidLimitException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.zalando.nakadi.service.EventStreamConfig.MAX_STREAM_TIMEOUT;

public class EventStreamConfigBuilderTest {

    @Test
    public void batchLimitZeroValueTest() throws InvalidLimitException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withBatchTimeout(0)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitDefaultValueTest() throws InvalidLimitException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitSpecifiedValueTest() throws InvalidLimitException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withBatchTimeout(29)
                .build();

        assertThat(config.getBatchTimeout(), is(29));
    }

    @Test(expected = InvalidLimitException.class)
    public void streamLimitLessThenBatchLimit() throws InvalidLimitException {
        EventStreamConfig
                .builder()
                .withBatchLimit(10)
                .withStreamLimit(1)
                .build();
    }

    @Test(expected = InvalidLimitException.class)
    public void streamTimeoutLessThenBatchTimeout() throws InvalidLimitException {
        EventStreamConfig
                .builder()
                .withBatchTimeout(10)
                .withStreamTimeout(1)
                .build();
    }

    @Test
    public void streamTimeoutHigherThanMax() throws InvalidLimitException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withStreamTimeout(MAX_STREAM_TIMEOUT + 100)
                .build();

        assertThat(config.getStreamTimeout(), lessThanOrEqualTo(MAX_STREAM_TIMEOUT));
    }

    @Test
    public void unlimitedStreamTimeout() throws InvalidLimitException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withStreamTimeout(0)
                .build();

        assertThat(config.getStreamTimeout(), greaterThan(0));
        assertThat(config.getStreamTimeout(), lessThanOrEqualTo(MAX_STREAM_TIMEOUT));
    }

}
