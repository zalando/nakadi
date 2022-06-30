package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.ConsumptionKpiCollector;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamParametersTest;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.autocommit.AutocommitSupport;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StreamingStateTest {

    private StreamingState state;

    private static final String SESSION_ID = "ssid";
    @Mock
    private ZkSubscriptionClient zkMock;
    @Mock
    private TimelineService timelineService;
    @Mock
    private Subscription subscription;
    @Mock
    private CursorConverter cursorConverter;
    @Mock
    private SubscriptionOutput out;
    @Mock
    private EventConsumer.ReassignableEventConsumer eventConsumer;
    @Mock
    private StreamingContext contextMock;
    @Mock
    private AutocommitSupport autocommitSupport;

    @Before
    public void prepareMocks() throws Exception {
        MockitoAnnotations.initMocks(this);

        state = new StreamingState();

        when(timelineService.createEventConsumer(Mockito.any())).thenReturn(eventConsumer);

        when(contextMock.getCursorComparator()).thenReturn(Comparator.comparing(NakadiCursor::getOffset));
        when(contextMock.getSessionId()).thenReturn(SESSION_ID);
        when(contextMock.isInState(Mockito.same(state))).thenReturn(true);

        when(contextMock.getSubscription()).thenReturn(subscription);
        when(contextMock.getTimelineService()).thenReturn(timelineService);
        when(contextMock.getOut()).thenReturn(out);
        doNothing().when(out).onInitialized(eq(SESSION_ID));

        when(contextMock.getMetricRegistry()).thenReturn(mock(MetricRegistry.class));
        when(contextMock.getZkClient()).thenReturn(zkMock);
        when(contextMock.getCursorConverter()).thenReturn(cursorConverter);
        when(contextMock.getCursorOperationsService())
                .thenReturn(Mockito.mock(CursorOperationsService.class));
        when(contextMock.getKpiCollector()).thenReturn(mock(ConsumptionKpiCollector.class));

        final Client client = mock(Client.class);
        when(client.getClientId()).thenReturn("consumingAppId");

        final StreamParameters spMock = StreamParametersTest.createStreamParameters(
                1000,
                100L,
                0L,
                100,
                100L,
                100,
                100,
                100,
                client
        );
        when(contextMock.getParameters()).thenReturn(spMock);

        when(contextMock.getAutocommitSupport()).thenReturn(autocommitSupport);

        state.setContext(contextMock);
    }

    @Test
    public void ensureTopologyEventListenerRegisteredRefreshedClosed() {
        final ZkSubscription topologySubscription = mock(ZkSubscription.class);
        Mockito.when(topologySubscription.getData())
                .thenReturn(new ZkSubscriptionClient.Topology(new Partition[]{}, 1));
        Mockito.when(zkMock.subscribeForTopologyChanges(Mockito.anyObject())).thenReturn(topologySubscription);

        state.onEnter();

        verify(zkMock, times(1)).subscribeForTopologyChanges(Mockito.any());
        verify(topologySubscription, times(1)).getData();

        state.reactOnTopologyChange();

        verify(topologySubscription, times(2)).getData();
        verify(topologySubscription, times(0)).close();

        state.onExit();

        verify(topologySubscription, times(1)).close();
        // verify that no new locks created.
        verify(zkMock, times(1)).subscribeForTopologyChanges(Mockito.any());
    }

    @Test
    public void ensureInitializationFailsWhenInvalidCursorsUsed() {
        final EventTypePartition pk = new EventTypePartition("t", "0");
        final Partition[] partitions = new Partition[]{new Partition(
                pk.getEventType(), pk.getPartition(), SESSION_ID, null, Partition.State.ASSIGNED)};

        // mock topology
        final ZkSubscription topologySubscription = mock(ZkSubscription.class);
        Mockito.when(topologySubscription.getData())
                .thenReturn(new ZkSubscriptionClient.Topology(partitions, 1));
        Mockito.when(zkMock.subscribeForTopologyChanges(Mockito.anyObject())).thenReturn(topologySubscription);

        // prepare mocks for assigned partitions
        final Storage storage = mock(Storage.class);
        when(storage.getType()).thenReturn(Storage.Type.KAFKA);
        final Timeline timeline = new Timeline("t", 0, storage, "t", new Date());
        final NakadiCursor anyCursor = NakadiCursor.of(timeline, "0", "0");

        when(cursorConverter.convert((SubscriptionCursorWithoutToken) any())).thenReturn(anyCursor);
        when(timelineService.getActiveTimelinesOrdered(eq("t"))).thenReturn(Collections.singletonList(timeline));
        final EventConsumer.ReassignableEventConsumer consumer = mock(EventConsumer.ReassignableEventConsumer.class);
        when(consumer.getAssignment()).thenReturn(Collections.emptySet());

        // Throw exception when reassigning partitions to consumer
        doThrow(new InvalidCursorException(CursorError.UNAVAILABLE, anyCursor)).when(consumer).reassign(any());
        when(timelineService.createEventConsumer(any())).thenReturn(consumer);
        when(subscription.getEventTypes()).thenReturn(Collections.singleton("t"));

        // mock beforeFirstCursor
        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(timelineService.getTopicRepository(eq(timeline))).thenReturn(topicRepository);
        final PartitionStatistics stats = mock(PartitionStatistics.class);
        final NakadiCursor beforeFirstCursor = NakadiCursor.of(timeline, "0", "0");
        when(stats.getBeforeFirst()).thenReturn(beforeFirstCursor);
        when(topicRepository.loadTopicStatistics(any())).thenReturn(Lists.newArrayList(stats));

        // enter state and expect InvalidCursorException
        state.onEnter();
        assertThrows(NakadiRuntimeException.class, () -> state.refreshTopologyUnlocked(partitions));
    }

    @Test
    public void ensureOffsetsSubscriptionsAreRefreshedAndClosed()
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException,
            InvalidCursorException {
        final ZkSubscription<SubscriptionCursorWithoutToken> offsetSubscription = mock(ZkSubscription.class);

        final EventTypePartition pk = new EventTypePartition("t", "0");
        Mockito.when(zkMock.subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any())).thenReturn(offsetSubscription);

        final EventConsumer.ReassignableEventConsumer consumer = mock(EventConsumer.ReassignableEventConsumer.class);
        when(consumer.getAssignment()).thenReturn(Collections.emptySet());
        when(timelineService.createEventConsumer(any())).thenReturn(consumer);
        when(subscription.getEventTypes()).thenReturn(Collections.singleton("t"));

        final Storage storage = mock(Storage.class);
        when(storage.getType()).thenReturn(Storage.Type.KAFKA);
        final Timeline timeline = new Timeline("t", 0, storage, "t", new Date());
        when(timelineService.getActiveTimelinesOrdered(eq("t"))).thenReturn(Collections.singletonList(timeline));
        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(timelineService.getTopicRepository(eq(timeline))).thenReturn(topicRepository);
        final PartitionStatistics stats = mock(PartitionStatistics.class);
        final NakadiCursor beforeFirstCursor = NakadiCursor.of(timeline, "0", "0");
        when(stats.getBeforeFirst()).thenReturn(beforeFirstCursor);
        when(topicRepository.loadTopicStatistics(any())).thenReturn(Lists.newArrayList(stats));

        state.onEnter();
        final NakadiCursor anyCursor = NakadiCursor.of(timeline, "0", "0");
        when(cursorConverter.convert((SubscriptionCursorWithoutToken)any())).thenReturn(anyCursor);

        state.refreshTopologyUnlocked(new Partition[]{
                new Partition(
                        pk.getEventType(), pk.getPartition(), SESSION_ID, null, Partition.State.ASSIGNED)});

        verify(zkMock, times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        verify(offsetSubscription, times(0)).close();
        verify(offsetSubscription, times(0)).getData();

        state.offsetChanged(pk);
        verify(zkMock, times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        verify(offsetSubscription, times(0)).close();
        verify(offsetSubscription, times(1)).getData();

        // Verify that offset change listener is removed
        state.refreshTopologyUnlocked(new Partition[0]);
        verify(zkMock, times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        verify(offsetSubscription, times(1)).close();
        verify(offsetSubscription, times(1)).getData();
    }
}
