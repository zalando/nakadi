package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.exceptions.UnprocessableEntityException;
import org.junit.Test;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EventStreamConfigBuilderTest {

    @Test
    public void batchLimitZeroValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic("test")
                .withBatchLimit(1)
                .withBatchTimeout(0)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitDefaultValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic("test")
                .withBatchLimit(1)
                .build();

        assertThat(config.getBatchTimeout(), is(30));
    }

    @Test
    public void batchLimitSpecifiedValueTest() throws UnprocessableEntityException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic("test")
                .withBatchLimit(1)
                .withBatchTimeout(29)
                .build();

        assertThat(config.getBatchTimeout(), is(29));
    }

}