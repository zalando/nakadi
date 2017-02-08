package org.zalando.nakadi.service;

import org.junit.Test;
import org.zalando.nakadi.exceptions.UnprocessableEntityException;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EventStreamConfigBuilderTest {

    @Test
    public void batchLimitZeroValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withBatchTimeout(0)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitDefaultValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitSpecifiedValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withBatchTimeout(29)
                .build();

        assertThat(config.getBatchTimeout(), is(29));
    }

    @Test(expected = UnprocessableEntityException.class)
    public void streamLimitLessThenBatchLimit() throws UnprocessableEntityException {
        EventStreamConfig
                .builder()
                .withBatchLimit(10)
                .withStreamLimit(1)
                .build();
    }

    @Test(expected = UnprocessableEntityException.class)
    public void streamTimeoutLessThenBatchTimeout() throws UnprocessableEntityException {
        EventStreamConfig
                .builder()
                .withBatchTimeout(10)
                .withStreamTimeout(1)
                .build();
    }

}