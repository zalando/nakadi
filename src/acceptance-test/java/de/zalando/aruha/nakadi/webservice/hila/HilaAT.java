package de.zalando.aruha.nakadi.webservice.hila;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.webservice.BaseAT;
import de.zalando.aruha.nakadi.webservice.utils.TestStreamingClient;
import org.junit.Test;

import java.util.stream.IntStream;

import static de.zalando.aruha.nakadi.utils.TestUtils.waitFor;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.commitCursors;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.createSubscription;
import static de.zalando.aruha.nakadi.webservice.utils.NakadiTestUtils.publishMessage;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;

public class HilaAT extends BaseAT {

    @Test(timeout = 30000)
    public void whenOffsetIsCommittedNextSessionStartsFromNextEventAfterCommitted() throws Exception {
        // create event-type and subscribe to it
        final EventType eventType = createEventType();
        final Subscription subscription = createSubscription(ImmutableSet.of(eventType.getName()));

        // write some events to event-type
        IntStream
                .rangeClosed(0, 3)
                .forEach(x -> publishMessage(eventType.getName(), "{\"blah\":" + x + "}"));

        // create session, read from subscription and wait for events to be sent
        final TestStreamingClient client = TestStreamingClient
                .create(URL, subscription.getId(), "stream_limit=2")
                .start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));

        // check that we have read first two events
        assertThat(client.getBatches().get(0).getCursor().getOffset(), equalTo("0"));
        assertThat(client.getBatches().get(1).getCursor().getOffset(), equalTo("1"));

        // commit offset that will also trigger session closing as we reached stream_limit and committed
        commitCursors(subscription.getId(), ImmutableList.of(client.getBatches().get(1).getCursor()));
        waitFor(() -> assertThat(client.isRunning(), is(false)));

        // create new session and read from subscription again
        client.start();
        waitFor(() -> assertThat(client.getBatches(), hasSize(2)));

        // check that we have read the next two events
        assertThat(client.getBatches().get(0).getCursor().getOffset(), equalTo("2"));
        assertThat(client.getBatches().get(1).getCursor().getOffset(), equalTo("3"));
    }

}
