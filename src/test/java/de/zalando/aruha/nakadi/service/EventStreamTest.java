package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.ConsumedEvent;
import de.zalando.aruha.nakadi.repository.kafka.NakadiKafkaConsumer;
import org.junit.Before;
import org.junit.Test;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Optional;

import static de.zalando.aruha.nakadi.utils.TestUtils.randomString;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomUInt;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventStreamTest {

    private static final String TOPIC = randomString();
    private static final int PARTITION = randomUInt();

    @Before
    public void setup() throws NakadiException {

    }

    @Test
    public void whenNoExitConditionsThenStreamIsNotClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be still alive as we didn't set any exit conditions", thread.isAlive(), is(true));
    }

    @Test
    public void whenStreamTimeoutIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withBatchLimit(1)
                .withStreamTimeout(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be closed as we set stream timeout to 1 second", thread.isAlive(), is(false));
    }

    @Test
    public void whenStreamLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamLimit(1)
                .build();
        final EventStream eventStream = new EventStream(endlessDummyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(1000);

        assertThat("The stream should be closed as we set stream limit to 1 event", thread.isAlive(), is(false));
    }

    @Test
    public void whenKeepAliveLimitIsSetThenStreamIsClosed() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .withStreamKeepAliveLimit(1)
                .build();
        final EventStream eventStream = new EventStream(emptyConsumer(), mock(OutputStream.class), config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(2000);

        assertThat("The stream should be closed as we set keep alive limit to 1", thread.isAlive(), is(false));
    }

    //@Test
    public void whenNoEventsToReadThenKeepAliveIsSent() throws NakadiException, IOException, InterruptedException {
        final EventStreamConfig config = EventStreamConfig
                .builder()
                .withTopic(TOPIC)
                .withCursors(ImmutableMap.of("0", "0"))
                .withBatchLimit(1)
                .build();

        final PipedInputStream pipeInput = new PipedInputStream();
        final BufferedReader reader = new BufferedReader(new InputStreamReader(pipeInput));
        final BufferedOutputStream out = new BufferedOutputStream(new PipedOutputStream(pipeInput));

        final EventStream eventStream = new EventStream(emptyConsumer(), out, config);

        final Thread thread = new Thread(eventStream::streamEvents);
        thread.start();
        Thread.sleep(3000);

        assertThat("The stream should be closed as we set keep alive limit to 1", thread.isAlive(), is(false));
    }

    private static NakadiKafkaConsumer emptyConsumer() throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenReturn(Optional.empty());
        return nakadiKafkaConsumer;
    }

    private static NakadiKafkaConsumer endlessDummyConsumer() throws NakadiException {
        final NakadiKafkaConsumer nakadiKafkaConsumer = mock(NakadiKafkaConsumer.class);
        when(nakadiKafkaConsumer.readEvent()).thenReturn(Optional.of(new ConsumedEvent("", TOPIC, "0", "0")));
        return nakadiKafkaConsumer;
    }
}
