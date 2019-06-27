package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.Comparator;
import java.util.Date;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.zalando.nakadi.service.subscription.StreamParametersTest.createStreamParameters;

public class StreamingStateTest {

    private StreamingState state;
    private ZkSubscriptionClient zkMock;

    private static final String SESSION_ID = "ssid";
    private TimelineService timelineService;
    private Subscription subscription;
    private CursorConverter cursorConverter;

    @Before
    public void prepareMocks() throws Exception {
        state = new StreamingState();

        final StreamingContext contextMock = mock(StreamingContext.class);
        when(contextMock.getCursorComparator()).thenReturn(Comparator.comparing(NakadiCursor::getOffset));

        when(contextMock.getSessionId()).thenReturn(SESSION_ID);
        when(contextMock.isInState(Mockito.same(state))).thenReturn(true);
        subscription = mock(Subscription.class);
        when(contextMock.getSubscription()).thenReturn(subscription);
        timelineService = mock(TimelineService.class);
        when(contextMock.getTimelineService()).thenReturn(timelineService);

        final MetricRegistry metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.register(any(), any())).thenReturn(null);
        when(contextMock.getMetricRegistry()).thenReturn(metricRegistry);

        zkMock = mock(ZkSubscriptionClient.class);
        when(contextMock.getZkClient()).thenReturn(zkMock);

        cursorConverter = mock(CursorConverter.class);
        when(contextMock.getCursorConverter()).thenReturn(cursorConverter);

        final Client client = mock(Client.class);
        when(client.getClientId()).thenReturn("consumingAppId");

        final StreamParameters spMock = createStreamParameters(
                1000,
                100L,
                100,
                100L,
                100,
                100,
                100,
                client
        );
        when(contextMock.getParameters()).thenReturn(spMock);

        state.setContext(contextMock);
    }

    @Test
    public void ensureTopologyEventListenerRegisteredRefreshedClosed() {
        final ZkSubscription topologySubscription = mock(ZkSubscription.class);
        Mockito.when(topologySubscription.getData())
                .thenReturn(new ZkSubscriptionClient.Topology(new Partition[]{}, null, 1));
        Mockito.when(zkMock.subscribeForTopologyChanges(Mockito.anyObject())).thenReturn(topologySubscription);

        state.onEnter();

        Mockito.verify(zkMock, Mockito.times(1)).subscribeForTopologyChanges(Mockito.any());
        Mockito.verify(topologySubscription, Mockito.times(1)).getData();

        state.reactOnTopologyChange();

        Mockito.verify(topologySubscription, Mockito.times(2)).getData();
        Mockito.verify(topologySubscription, Mockito.times(0)).close();

        state.onExit();

        Mockito.verify(topologySubscription, Mockito.times(1)).close();
        // verify that no new locks created.
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForTopologyChanges(Mockito.any());
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
        when(cursorConverter.convert(any(SubscriptionCursorWithoutToken.class))).thenReturn(anyCursor);

        state.refreshTopologyUnlocked(new Partition[]{
                new Partition(
                        pk.getEventType(), pk.getPartition(), SESSION_ID, null, Partition.State.ASSIGNED)});

        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(0)).close();
        Mockito.verify(offsetSubscription, Mockito.times(0)).getData();

        state.offsetChanged(pk);
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(0)).close();
        Mockito.verify(offsetSubscription, Mockito.times(1)).getData();

        // Verify that offset change listener is removed
        state.refreshTopologyUnlocked(new Partition[0]);
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(1)).close();
        Mockito.verify(offsetSubscription, Mockito.times(1)).getData();
    }

}
