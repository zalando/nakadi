package org.zalando.nakadi.service.subscription.state;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;
import org.apache.kafka.clients.consumer.Consumer;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.service.subscription.KafkaClient;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.StreamingContext;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.view.SubscriptionCursor;

import static junit.framework.TestCase.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class StreamingStateTest {

    private StreamingState state;
    private KafkaClient kafkaMock;
    private ZkSubscriptionClient zkMock;

    private static final String SESSION_ID = "ssid";
    private MetricRegistry metricRegistry;

    final ObjectMapper mapper = new JsonConfig().jacksonObjectMapper();

    @Before
    public void prepareMocks() {
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

    @Test
    public void testWriteStreamBatch() {

        final StreamingState state = new StreamingState();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Meter meter = mock(Meter.class);
        final SubscriptionOutput so = new SubscriptionOutput() {
            @Override public void onInitialized(String sessionId) throws IOException { }

            @Override public void onException(Exception ex) {
                fail(ex.getMessage());
            }

            @Override public void streamData(byte[] data) throws IOException {
                baos.write(data);
                baos.flush();
            }
        };

        final SortedMap<Long, String> events = new TreeMap<>();
        events.put(0L, "{\"a\":\"b\"}");
        events.put(1L, "{\"c\":\"d\"}");
        events.put(2L, "{\"e\":\"f\"}");

        final String logline = "log-message";
        final String partition = "12";
        final String offset = "000000000000000023";
        final String eventType = "et1";
        final String cursorToken = "ct1";
        final SubscriptionCursor cursor =
            new SubscriptionCursor(partition, offset, eventType, cursorToken);

        try {
            state.writeStreamBatch(
                events,
                Optional.of(logline),
                cursor,
                so, mapper,
                meter
            );

            final String json = baos.toString();

            // there's only one newline and it's at the end
            final int newlineCount = (json).replaceAll("[^\n]", "").length();
            assertTrue(newlineCount == 1);
            assertTrue(json.endsWith("\n"));

            final Map<String, Object> batchM =
                mapper.readValue(baos.toString(), new TypeReference<Map<String, Object>>() {});

            final Map<String, String> cursorM = (Map<String, String>) batchM.get("cursor");

            assertEquals(partition, cursorM.get("partition"));
            assertEquals(offset, cursorM.get("offset"));
            assertEquals(eventType, cursorM.get("event_type"));
            assertEquals(cursorToken, cursorM.get("cursor_token"));

            final List<Map<String, String>> eventsM = (List<Map<String, String>>) batchM.get("events");
            assertTrue(eventsM.size() == 3);

            // check the order is preserved as well as the data via get
            assertEquals("b", eventsM.get(0).get("a"));
            assertEquals("d", eventsM.get(1).get("c"));
            assertEquals("f", eventsM.get(2).get("e"));

            verify(meter, times(1)).mark(anyInt());

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

    @Test
    public void testWriteStreamEventEmptyBatchProducesNoEventArray() {

        final StreamingState state = new StreamingState();
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final Meter meter = mock(Meter.class);
        final SubscriptionOutput so = new SubscriptionOutput() {
            @Override public void onInitialized(String sessionId) throws IOException { }

            @Override public void onException(Exception ex) {
                fail(ex.getMessage());
            }

            @Override public void streamData(byte[] data) throws IOException {
                baos.write(data);
                baos.flush();
            }
        };

        final SortedMap<Long, String> events = new TreeMap<>();
        final String logline = "log-message";
        final String partition = "12";
        final String offset = "000000000000000023";
        final String eventType = "et1";
        final String cursorToken = "ct1";
        final SubscriptionCursor cursor =
            new SubscriptionCursor(partition, offset, eventType, cursorToken);

        try {
            state.writeStreamBatch(
                events,
                Optional.of(logline),
                cursor,
                so, mapper,
                meter
            );

            final String json = baos.toString();

            // there's only one newline and it's at the end
            final int newlineCount = (json).replaceAll("[^\n]", "").length();
            assertTrue(newlineCount == 1);
            assertTrue(json.endsWith("\n"));

            final Map<String, Object> batchM =
                mapper.readValue(baos.toString(), new TypeReference<Map<String, Object>>() {});

            final Map<String, String> cursorM = (Map<String, String>) batchM.get("cursor");

            assertEquals(partition, cursorM.get("partition"));
            assertEquals(offset, cursorM.get("offset"));
            assertEquals(eventType, cursorM.get("event_type"));
            assertEquals(cursorToken, cursorM.get("cursor_token"));

            final List<Map<String, String>> eventsM = (List<Map<String, String>>) batchM.get("events");
            // did not write an empty batch
            assertFalse(json.contains("\"events\""));
            assertTrue(eventsM == null);

            verify(meter, times(1)).mark(anyInt());

        } catch (IOException e) {
            fail(e.getMessage());
        }
    }

}
