package de.zalando.aruha.nakadi.service;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Topology;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.utils.LocalSubscriptionRepository;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.OutputStream;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventStreamCoordinatorTest {

    private static final String SUBSCRIPTION_ID = "sub1";

    private SubscriptionRepository subscriptionRepository;
    private TopicRepository topicRepository;
    private EventStreamCoordinator coordinator;

    @Before
    public void setup() {
        subscriptionRepository = new LocalSubscriptionRepository();
        topicRepository = mock(TopicRepository.class);
        coordinator = new EventStreamCoordinator(subscriptionRepository,
                new RegularPartitionDistributor(topicRepository, subscriptionRepository), topicRepository);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenCreateSecondStreamForSubscriptionThenOk() {
        // ARRANGE //
        final String client1Id = "!client1"; // this client will be alphabetically lower than new client id
        subscriptionRepository.createSubscription(
                SUBSCRIPTION_ID,
                ImmutableList.of("topic1", "topic2"),
                ImmutableList.of(
                        new Cursor("topic1", "0", "offset-t1-p0"),
                        new Cursor("topic1", "1", "offset-t1-p1"),
                        new Cursor("topic2", "0", "offset-t2-p0"),
                        new Cursor("topic2", "1", "offset-t2-p1"),
                        new Cursor("topic2", "2", "offset-t2-p2"),
                        new Cursor("topic2", "3", "offset-t2-p3")
                ));
        subscriptionRepository.addClient(SUBSCRIPTION_ID, client1Id);

        Class<List<Cursor>> cursorListClass = (Class<List<Cursor>>) (Class) List.class;
        final ArgumentCaptor<List<Cursor>> cursorsCaptor = ArgumentCaptor.forClass(cursorListClass);
        when(topicRepository.createEventConsumer(cursorsCaptor.capture())).thenReturn(mock(EventConsumer.class));

        when(topicRepository.listPartitions("topic1")).thenReturn(ImmutableList.of("0", "1"));
        when(topicRepository.listPartitions("topic2")).thenReturn(ImmutableList.of("0", "1", "2", "3"));

        // ACT //
        final EventStream eventStream = coordinator.createEventStream(SUBSCRIPTION_ID, mock(OutputStream.class),
                EventStreamConfig.builder().build());

        // ASSERT //
        assertThat("Correct subscriptionId should be set for new event stream",
                eventStream.getSubscriptionId(), equalTo(SUBSCRIPTION_ID));

        final String newClientId = eventStream.getClientId();
        final Topology topology = subscriptionRepository.getTopology(SUBSCRIPTION_ID);
        assertThat("New client should be added to topology", topology.getClientIds(),
                equalTo(ImmutableList.of(client1Id, newClientId)));

        final List<Cursor> cursors = cursorsCaptor.getValue();
        assertThat("Correct cursors should be set for new stream", cursors, equalTo(ImmutableList.of(
                new Cursor("topic1", "1", "offset-t1-p1"),
                new Cursor("topic2", "1", "offset-t2-p1"),
                new Cursor("topic2", "3", "offset-t2-p3")
        )));
    }

    @Test
    public void testWhenRemoveStreamThenOk() {
        // ARRANGE //
        subscriptionRepository.createSubscription(
                SUBSCRIPTION_ID,
                ImmutableList.of("topic1"),
                ImmutableList.of(new Cursor("topic1", "0", "0")));
        subscriptionRepository.addClient(SUBSCRIPTION_ID, "client1Id");
        subscriptionRepository.addClient(SUBSCRIPTION_ID, "client2Id");

        final EventStream eventStream = new EventStream(mock(EventConsumer.class), mock(OutputStream.class),
                EventStreamConfig.builder().build(), ImmutableList.of());
        eventStream.setSubscriptionId(SUBSCRIPTION_ID);
        eventStream.setClientId("client1Id");

        // ACT //
        coordinator.removeEventStream(eventStream);

        // ASSERT //
        final Topology topology = subscriptionRepository.getTopology(SUBSCRIPTION_ID);
        assertThat("Removed client should be removed from topology", topology.getClientIds(),
                equalTo(ImmutableList.of("client2Id")));
    }
}
