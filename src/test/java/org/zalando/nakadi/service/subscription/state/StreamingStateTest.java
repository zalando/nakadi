package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.MetricRegistry;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.service.subscription.KafkaClient;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;

import java.util.Collections;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;

public class StreamingStateTest {

    private StreamingState state;
    private KafkaClient kafkaMock;
    private ZkSubscriptionClient zkMock;

    private static final String SESSION_ID = "ssid";
    private MetricRegistry metricRegistry;

    @Before
    public void prepareMocks() throws Exception {
        state = new StreamingState();

        final StreamingContext contextMock = mock(StreamingContext.class);

        Mockito.when(contextMock.getSessionId()).thenReturn(SESSION_ID);
        Mockito.when(contextMock.isInState(Mockito.same(state))).thenReturn(true);

        kafkaMock = mock(KafkaClient.class);
        Mockito.when(contextMock.getKafkaClient()).thenReturn(kafkaMock);

        metricRegistry = mock(MetricRegistry.class);
        Mockito.when(metricRegistry.register(any(), any())).thenReturn(null);
        Mockito.when(contextMock.getMetricRegistry()).thenReturn(metricRegistry);

        zkMock = mock(ZkSubscriptionClient.class);
        Mockito.when(contextMock.getZkClient()).thenReturn(zkMock);

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
    public void ensureOffsetsSubscriptionsAreRefreshedAndClosed() {
        final ZKSubscription offsetSubscription = mock(ZKSubscription.class);

        final Partition.PartitionKey pk = new Partition.PartitionKey("t", "0");
        Mockito.when(zkMock.subscribeForOffsetChanges(Mockito.eq(pk), Mockito.any())).thenReturn(offsetSubscription);

        final Consumer kafkaConsumer = mock(Consumer.class);
        Mockito.when(kafkaConsumer.assignment()).thenReturn(Collections.emptySet());
        Mockito.when(kafkaMock.createKafkaConsumer()).thenReturn(kafkaConsumer);

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
