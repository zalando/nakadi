package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.view.UserStreamParameters;

import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.number.OrderingComparison.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;

public class StreamParametersTest {

    @Test(expected = WrongStreamParametersException.class)
    public void whenBatchLimitLowerOrEqualToZeroTheException() throws Exception {
        createStreamParameters(0, null, 0, null, null, 0, 0, mock(Client.class));
    }

    @Test
    public void checkParamsAreTransformedCorrectly() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, null, 10, 60L, null, 1000, 20,
                mock(Client.class));

        assertThat(streamParameters.batchLimitEvents, equalTo(1));
        assertThat(streamParameters.batchTimeoutMillis, equalTo(10000L));
        assertThat(streamParameters.streamTimeoutMillis, equalTo(60000L));
        assertThat(streamParameters.maxUncommittedMessages, equalTo(1000));
        assertThat(streamParameters.commitTimeoutMillis, equalTo(20000L));
    }

    @Test
    public void whenStreamTimeoutOmittedThenItIsGenerated() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, null, 0, null, null, 0, 0,
                mock(Client.class));

        checkStreamTimeoutIsGeneratedCorrectly(streamParameters);
    }

    @Test
    public void whenStreamTimeoutIsGreaterThanMaxThenItIsGenerated() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, null, 0,
                EventStreamConfig.MAX_STREAM_TIMEOUT + 1L, null, 0, 0, mock(Client.class));

        checkStreamTimeoutIsGeneratedCorrectly(streamParameters);
    }

    @Test
    public void checkIsStreamLimitReached() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, 150L, 0, null, null, 0, 0,
                mock(Client.class));

        assertThat(streamParameters.isStreamLimitReached(140), is(false));
        assertThat(streamParameters.isStreamLimitReached(151), is(true));
        assertThat(streamParameters.isStreamLimitReached(150), is(true));
    }

    @Test
    public void checkIsKeepAliveLimitReached() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, null, 0, null, 5, 0, 0, mock(Client.class));

        assertThat(streamParameters.isKeepAliveLimitReached(IntStream.of(5, 7, 6, 12)), is(true));
        assertThat(streamParameters.isKeepAliveLimitReached(IntStream.of(5, 7, 4, 12)), is(false));
    }

    @Test
    public void checkGetMessagesAllowedToSend() throws Exception {
        final StreamParameters streamParameters = createStreamParameters(1, 200L, 0, null, null, 0, 0,
                mock(Client.class));

        assertThat(streamParameters.getMessagesAllowedToSend(50, 190), equalTo(10L));
        assertThat(streamParameters.getMessagesAllowedToSend(50, 120), equalTo(50L));
    }

    private void checkStreamTimeoutIsGeneratedCorrectly(final StreamParameters streamParameters) {
        assertThat(streamParameters.streamTimeoutMillis, lessThanOrEqualTo(
                TimeUnit.SECONDS.toMillis((long) EventStreamConfig.MAX_STREAM_TIMEOUT)));
        assertThat(streamParameters.streamTimeoutMillis, greaterThanOrEqualTo(
                TimeUnit.SECONDS.toMillis((long) EventStreamConfig.MAX_STREAM_TIMEOUT - 1200)));
    }

    public static StreamParameters createStreamParameters(final int batchLimitEvents,
                                                          final Long streamLimitEvents,
                                                          final int batchTimeoutSeconds,
                                                          final Long streamTimeoutSeconds,
                                                          final Integer batchKeepAliveIterations,
                                                          final int maxUncommittedMessages,
                                                          final long commitTimeoutSeconds,
                                                          final Client client) throws WrongStreamParametersException {
        final UserStreamParameters userParams = new UserStreamParameters(batchLimitEvents, streamLimitEvents,
                batchTimeoutSeconds, streamTimeoutSeconds, batchKeepAliveIterations, maxUncommittedMessages,
                ImmutableList.of(), commitTimeoutSeconds);
        return StreamParameters.of(userParams, commitTimeoutSeconds, client);
    }
}
