package org.zalando.nakadi.service.subscription;

import org.junit.Test;
import org.zalando.nakadi.exceptions.UnprocessableEntityException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventStreamConfig;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class StreamParametersTest {

    @Test(expected = UnprocessableEntityException.class)
    public void whenBatchLimitLowerOrEqualToZeroTheException() throws Exception {
        StreamParameters.of(0, null, 0, null, null, 0, 0, mock(Client.class));
    }

    @Test
    public void checkParamsAreTransformedCorrectly() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, null, 10, 60L, null, 1000, 20,
                mock(Client.class));

        assertThat(streamParameters.batchLimitEvents, equalTo(1));
        assertThat(streamParameters.batchTimeoutMillis, equalTo(10000L));
        assertThat(streamParameters.streamTimeoutMillis, equalTo(60000L));
        assertThat(streamParameters.maxUncommittedMessages, equalTo(1000));
        assertThat(streamParameters.commitTimeoutMillis, equalTo(20000L));
    }

    @Test
    public void whenStreamTimeoutOmittedThenItIsGenerated() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, null, 0, null, null, 0, 0, mock(Client.class));

        checkStreamTimeoutIsGeneratedCorrectly(streamParameters);
    }

    @Test
    public void whenStreamTimeoutIsGreaterThanMaxThenItIsGenerated() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, null, 0,
                EventStreamConfig.MAX_STREAM_TIMEOUT + 1L, null, 0, 0, mock(Client.class));

        checkStreamTimeoutIsGeneratedCorrectly(streamParameters);
    }

    @Test
    public void checkIsStreamLimitReached() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, 150L, 0, null, null, 0, 0, mock(Client.class));

        assertThat(streamParameters.isStreamLimitReached(140), is(false));
        assertThat(streamParameters.isStreamLimitReached(151), is(true));
        assertThat(streamParameters.isStreamLimitReached(150), is(true));
    }

    @Test
    public void checkIsKeepAliveLimitReached() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, null, 0, null, 5, 0, 0, mock(Client.class));

        assertThat(streamParameters.isKeepAliveLimitReached(IntStream.of(5, 7, 6, 12)), is(true));
        assertThat(streamParameters.isKeepAliveLimitReached(IntStream.of(5, 7, 4, 12)), is(false));
    }

    @Test
    public void checkgetMessagesAllowedToSend() throws Exception {
        final StreamParameters streamParameters = StreamParameters.of(1, 200L, 0, null, null, 0, 0, mock(Client.class));

        assertThat(streamParameters.getMessagesAllowedToSend(50, 190), equalTo(10L));
        assertThat(streamParameters.getMessagesAllowedToSend(50, 120), equalTo(50L));
    }

    private void checkStreamTimeoutIsGeneratedCorrectly(final StreamParameters streamParameters) {
        assertThat(streamParameters.streamTimeoutMillis, lessThanOrEqualTo(
                TimeUnit.SECONDS.toMillis((long) EventStreamConfig.MAX_STREAM_TIMEOUT)));
        assertThat(streamParameters.streamTimeoutMillis, greaterThanOrEqualTo(
                TimeUnit.SECONDS.toMillis((long) EventStreamConfig.MAX_STREAM_TIMEOUT - 1200)));
    }
}
