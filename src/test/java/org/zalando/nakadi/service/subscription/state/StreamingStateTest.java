package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.MetricRegistry;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.Collections;
import java.util.Date;
import java.util.Optional;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

        when(contextMock.getSessionId()).thenReturn(SESSION_ID);
        when(contextMock.isInState(Mockito.same(state))).thenReturn(true);
        subscription = mock(Subscription.class);
        when(contextMock.getSubscription()).thenReturn(subscription);
        timelineService = mock(TimelineService.class);
        when(contextMock.getTimelineService()).thenReturn(timelineService);

        final MetricRegistry metricRegistry = mock(MetricRegistry.class);
        when(metricRegistry.register(any(), any())).thenReturn(null);
        when(contextMock.getMetricRegistry()).thenReturn(metricRegistry);

        final SubscriptionCursorWithoutToken cursor = mock(SubscriptionCursorWithoutToken.class);
        zkMock = mock(ZkSubscriptionClient.class);
        when(contextMock.getZkClient()).thenReturn(zkMock);
        when(zkMock.getOffset(any())).thenReturn(cursor);

        cursorConverter = mock(CursorConverter.class);
        when(contextMock.getCursorConverter()).thenReturn(cursorConverter);

        final StreamParameters spMock = StreamParameters.of(
                1000,
                100L,
                100,
                100L,
                100,
                100,
                100,
                "consumingAppId"
        );
        when(contextMock.getParameters()).thenReturn(spMock);

        state.setContext(contextMock, "test");
    }

    @Test
    public void ensureTopologyEventListenerRegisteredRefreshedClosed() {
        final ZKSubscription topologySubscription = mock(ZKSubscription.class);
        Mockito.when(zkMock.subscribeForTopologyChanges(Mockito.anyObject())).thenReturn(topologySubscription);

        state.onEnter();

        Mockito.verify(zkMock, Mockito.times(1)).subscribeForTopologyChanges(Mockito.any());
        Mockito.verify(topologySubscription, Mockito.times(0)).refresh();

        state.topologyChanged();

        Mockito.verify(topologySubscription, Mockito.times(1)).refresh();
        Mockito.verify(topologySubscription, Mockito.times(0)).cancel();

        state.onExit();

        Mockito.verify(topologySubscription, Mockito.times(1)).cancel();
        // verify that no new locks created.
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForTopologyChanges(Mockito.any());
    }

    @Test
    public void ensureOffsetsSubscriptionsAreRefreshedAndClosed()
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException,
            InvalidCursorException {
        final ZKSubscription offsetSubscription = mock(ZKSubscription.class);

        final EventTypePartition pk = new EventTypePartition("t", "0");
        Mockito.when(zkMock.subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any())).thenReturn(offsetSubscription);

        final EventConsumer.ReassignableEventConsumer consumer = mock(EventConsumer.ReassignableEventConsumer.class);
        when(consumer.getAssignment()).thenReturn(Collections.emptySet());
        when(timelineService.createEventConsumer(any())).thenReturn(consumer);
        when(subscription.getEventTypes()).thenReturn(Collections.singleton("t"));

        final Timeline timeline = new Timeline("t", 0, null, "t", new Date());
        when(timelineService.getActiveTimelinesOrdered(eq("t"))).thenReturn(Collections.singletonList(timeline));
        final TopicRepository topicRepository = mock(TopicRepository.class);
        when(timelineService.getTopicRepository(eq(timeline))).thenReturn(topicRepository);
        final PartitionStatistics stats = mock(PartitionStatistics.class);
        when(stats.getBeforeFirst()).thenReturn(new NakadiCursor(timeline, "0", "0"));
        when(topicRepository.loadPartitionStatistics(eq(timeline), eq("0"))).thenReturn(Optional.of(stats));

        state.onEnter();

        when(cursorConverter.convert(any(SubscriptionCursorWithoutToken.class))).thenReturn(
                new NakadiCursor(timeline, "0", "0"));

        state.refreshTopologyUnlocked(new Partition[]{
                new Partition(
                        pk.getEventType(), pk.getPartition(), SESSION_ID, null, Partition.State.ASSIGNED)});

        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(0)).cancel();
        Mockito.verify(offsetSubscription, Mockito.times(0)).refresh();

        state.offsetChanged(pk);
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(0)).cancel();
        Mockito.verify(offsetSubscription, Mockito.times(1)).refresh();

        // Verify that offset change listener is removed
        state.refreshTopologyUnlocked(new Partition[0]);
        Mockito.verify(zkMock, Mockito.times(1)).subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any());
        Mockito.verify(offsetSubscription, Mockito.times(1)).cancel();
        Mockito.verify(offsetSubscription, Mockito.times(1)).refresh();
    }

}
