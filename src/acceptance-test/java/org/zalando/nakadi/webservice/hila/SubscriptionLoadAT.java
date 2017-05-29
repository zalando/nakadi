package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import com.jayway.restassured.response.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.http.HttpStatus;
import static org.hamcrest.Matchers.hasSize;
import org.junit.Assert;
import static org.junit.Assert.assertThat;
import org.junit.Test;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.Try;
import org.zalando.nakadi.util.TimeLogger;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.webservice.BaseAT;
import static org.zalando.nakadi.webservice.hila.SubscriptionAT.SUBSCRIPTIONS_URL;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

public class SubscriptionLoadAT {

    private static final int EVENT_TYPE_COUNT = 2;
    private static final int PARTITION_PER_EVENT_TYPE = 8;
    private static final int MESSAGES_PER_EVENT_TYPE = PARTITION_PER_EVENT_TYPE * 10;
    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();

    @Test(timeout = 30000)
    public void testSubscriptionLoad() throws IOException, InterruptedException {
        // create event types
        List<TestStreamingClient> clients = null;
        TimeLogger.startMeasure("testSubscriptionLoad", "createEventTypes");
        try {
            final List<EventType> eventTypeList = createEventTypes(EVENT_TYPE_COUNT, PARTITION_PER_EVENT_TYPE);

            // publish events to each event type
            TimeLogger.addMeasure("Publishing " + (MESSAGES_PER_EVENT_TYPE * EVENT_TYPE_COUNT) + " messages");
            eventTypeList.parallelStream()
                    .forEach(et -> sendEvents(et, MESSAGES_PER_EVENT_TYPE, PARTITION_PER_EVENT_TYPE));

            // create subscription
            TimeLogger.addMeasure("Create subscription");
            final Subscription subscription = createSubscription(eventTypeList);

            TimeLogger.addMeasure("Create clients");
            clients = createSteamingClients(
                    subscription, Math.min(EVENT_TYPE_COUNT + 5, EVENT_TYPE_COUNT * PARTITION_PER_EVENT_TYPE));
            clients.forEach(TestStreamingClient::start);

            // stream all the data.
            TimeLogger.addMeasure("Receive and commit events");
            final Map<String, String> receivedEvents = streamAllData(
                    clients, MESSAGES_PER_EVENT_TYPE * EVENT_TYPE_COUNT);

            // Close all connections
            TimeLogger.addMeasure("Closing connections");
            clients.parallelStream().forEach(TestStreamingClient::close);

            TimeLogger.addMeasure("checking");
            // Check total events count
            Assert.assertEquals(EVENT_TYPE_COUNT * MESSAGES_PER_EVENT_TYPE, receivedEvents.size());
            // Check that every consumer received at least something
            clients.stream().map(TestStreamingClient::getSessionId).forEach(
                    sessionId -> Assert.assertTrue(
                            receivedEvents.values().stream().filter(v -> v.equals(sessionId)).count() > 0));
        } finally {
            TimeLogger.addMeasure("stop_clients");
            if (null != clients) {
                clients.forEach(TestStreamingClient::close);
            }
            System.out.println(TimeLogger.finishMeasure());
        }
    }

    private static SubscriptionCursor asNoTokenCursor(final SubscriptionCursor source) {
        return new SubscriptionCursor(
                source.getPartition(),
                source.getOffset(),
                source.getEventType(),
                "");
    }

    @Test(timeout = 15000)
    public void testCursorsForDifferentEventTypesAreCommittedAtATime() throws IOException {
        final EventType[] eventTypes = new EventType[]{
                NakadiTestUtils.createBusinessEventTypeWithPartitions(2),
                NakadiTestUtils.createBusinessEventTypeWithPartitions(1)};

        final Subscription subscription = createSubscription(Arrays.asList(eventTypes));

        sendEvents(eventTypes[0], 2, 2);
        sendEvents(eventTypes[1], 1, 1);

        final TestStreamingClient client = createSteamingClients(subscription, 1).get(0).start();

        waitFor(() -> assertThat(client.getBatches(), hasSize(3))); // 3 events

        final List<SubscriptionCursor> receivedCursors = client.getBatches().stream()
                .map(StreamBatch::getCursor)
                .filter(c -> !c.getOffset().equals("BEGIN"))
                .collect(Collectors.toList());
        Assert.assertEquals(3, receivedCursors.size());

        NakadiTestUtils.commitCursors(subscription.getId(), receivedCursors, client.getSessionId());
        final Comparator<SubscriptionCursor> comparator = Comparator.comparing(SubscriptionCursor::getEventType)
                .thenComparing(SubscriptionCursor::getPartition);

        final List<SubscriptionCursor> actualCursors = NakadiTestUtils.getSubscriptionCursors(subscription).getItems()
                .stream()
                .map(SubscriptionLoadAT::asNoTokenCursor)
                .sorted(comparator)
                .collect(Collectors.toList());
        final List<SubscriptionCursor> expectedCursors = receivedCursors.stream()
                .map(SubscriptionLoadAT::asNoTokenCursor)
                .sorted(comparator)
                .collect(Collectors.toList());
        Assert.assertEquals(expectedCursors, actualCursors);

        // Add 1 event and expect to see it
        sendEvents(eventTypes[1], 1, 1);
        waitFor(() -> assertThat(client.getBatches(), hasSize(4)));
        final SubscriptionCursor added1 = client.getBatches().get(3).getCursor();
        Assert.assertEquals(eventTypes[1].getName(), added1.getEventType());

        final List<SubscriptionCursor> newExpectedCursors =
                Stream.concat(
                        actualCursors.stream().filter(c -> !c.getEventType().equals(eventTypes[1].getName())),
                        Stream.of(added1)
                )
                        .map(SubscriptionLoadAT::asNoTokenCursor)
                        .sorted(comparator)
                        .collect(Collectors.toList());

        NakadiTestUtils.commitCursors(subscription.getId(), Collections.singletonList(added1), client.getSessionId());

        final List<SubscriptionCursor> actualCursors2 =
                NakadiTestUtils.getSubscriptionCursors(subscription).getItems().stream()
                        .map(SubscriptionLoadAT::asNoTokenCursor)
                        .sorted(comparator)
                        .collect(Collectors.toList());
        Assert.assertEquals(newExpectedCursors, actualCursors2);
        client.close();
    }

    private Map<String, String> streamAllData(final List<TestStreamingClient> clients, final int totalEvents)
            throws InterruptedException {

        final Map<String, Integer> clientsBatchCount = new HashMap<>();
        final Map<String, String> events = new HashMap<>();
        while (events.size() < totalEvents) {
            Thread.sleep(100);
            clients.forEach(client -> {
                final List<StreamBatch> batches = client.getBatches();
                if (!batches.isEmpty()) {
                    if (!clientsBatchCount.containsKey(client.getSessionId())) {
                        clientsBatchCount.put(client.getSessionId(), 0);
                    }
                    final int batchesReceived = batches.size();
                    final List<StreamBatch> addedBatches = batches.subList(
                            clientsBatchCount.get(client.getSessionId()),
                            batchesReceived);
                    final int eventsInClient = batches.stream().mapToInt(b -> b.getEvents().size()).sum();
                    if (!addedBatches.isEmpty() && eventsInClient > 0) {
                        // commit new data
                        try {
                            final Map<String, List<SubscriptionCursor>> cursors = addedBatches.stream()
                                    .filter(batch -> !batch.getCursor().getOffset().equals("BEGIN"))
                                    .map(StreamBatch::getCursor).collect(
                                            Collectors.groupingBy(c -> c.getEventType() + "_" + c.getPartition()));
                            cursors.values().forEach(joinedCursors -> joinedCursors.sort(
                                    Comparator.comparing(f -> Integer.valueOf(f.getOffset()))));
                            NakadiTestUtils.commitCursors(
                                    client.getSubscriptionId(),
                                    cursors.values().stream().map(c -> c.get(0)).collect(Collectors.toList()),
                                    client.getSessionId());
                            clientsBatchCount.put(client.getSessionId(), batchesReceived);
                            addedBatches.stream()
                                    .flatMap(batch -> batch.getEvents().stream())
                                    .forEach(evt -> {
                                        final String foo = (String) evt.get("foo");
                                        Assert.assertFalse(events.containsKey(foo));
                                        events.put(foo, client.getSessionId());
                                    });
                        } catch (JsonProcessingException ex) {
                            throw new RuntimeException(ex);
                        }
                    }
                }
            });
        }
        return events;
    }

    private static List<TestStreamingClient> createSteamingClients(
            final Subscription subscription, final int consumerCount) {
        final String streamParams = "stream_limit=" + EVENT_TYPE_COUNT * MESSAGES_PER_EVENT_TYPE +
                "&batch_size=1&batch_flush_timeout=1";
        return IntStream.range(0, consumerCount)
                .mapToObj(index -> TestStreamingClient.create(BaseAT.URL, subscription.getId(), streamParams))
                .collect(Collectors.toList());
    }

    private Subscription createSubscription(final List<EventType> eventTypeList) throws IOException {
        final String subscriptionStr = "{\"owning_application\":\"app\",\"read_from\":\"BEGIN\",\"event_types\":[" +
                eventTypeList.stream()
                        .map(EventType::getName)
                        .map(name -> "\"" + name + "\"")
                        .collect(Collectors.joining(","))
                + "]}";

        final Response response = given()
                .body(subscriptionStr)
                .contentType(JSON)
                .post(SUBSCRIPTIONS_URL);

        response.then().statusCode(HttpStatus.SC_CREATED);

        return MAPPER.readValue(response.print(), Subscription.class);
    }

    private static void sendEvents(final EventType eventType, final int eventCount, final int partitions) {
        final Map<String, String> data = IntStream.range(0, eventCount).boxed().collect(
                Collectors.toMap(
                        idx -> "et-" + eventType.getName() + "-" + idx,
                        idx -> String.valueOf(idx % partitions)));
        NakadiTestUtils.publishBusinessEventsWithUserDefinedPartition(eventType.getName(), data);
    }

    private static List<EventType> createEventTypes(final int count, final int partitions) {
        return IntStream.range(0, count)
                .boxed()
                .map(Try.wrap(index ->
                        NakadiTestUtils.createBusinessEventTypeWithPartitions(partitions)).andThen(Try::getOrThrow))
                .collect(Collectors.toList());
    }

}
