package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.MetricRegistry;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.TopicPartition;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.service.timeline.TimelineService;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StreamingStateTest {

    private StreamingState state;
    private ZkSubscriptionClient zkMock;

    private static final String SESSION_ID = "ssid";
    private MetricRegistry metricRegistry;
    private TimelineService timelineService;
    private Subscription subscription;

    @Before
    public void prepareMocks() {
        state = new StreamingState();

        final StreamingContext contextMock = mock(StreamingContext.class);

        Mockito.when(contextMock.getSessionId()).thenReturn(SESSION_ID);
        Mockito.when(contextMock.isInState(Mockito.same(state))).thenReturn(true);
        subscription = mock(Subscription.class);
        Mockito.when(contextMock.getSubscription()).thenReturn(subscription);
        timelineService = mock(TimelineService.class);
        Mockito.when(contextMock.getTimelineService()).thenReturn(timelineService);

        metricRegistry = mock(MetricRegistry.class);
        Mockito.when(metricRegistry.register(any(), any())).thenReturn(null);
        Mockito.when(contextMock.getMetricRegistry()).thenReturn(metricRegistry);

        zkMock = mock(ZkSubscriptionClient.class);
        Mockito.when(contextMock.getZkClient()).thenReturn(zkMock);
        when(zkMock.getOffset(any())).thenReturn("0");

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
        Mockito.when(contextMock.getParameters()).thenReturn(spMock);

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
            throws InternalNakadiException, NoSuchEventTypeException, ServiceUnavailableException {
        final ZKSubscription offsetSubscription = mock(ZKSubscription.class);

        final TopicPartition pk = new TopicPartition("t", "0");
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

        state.refreshTopologyUnlocked(new Partition[]{new Partition(pk, SESSION_ID, null, Partition.State.ASSIGNED)});

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
