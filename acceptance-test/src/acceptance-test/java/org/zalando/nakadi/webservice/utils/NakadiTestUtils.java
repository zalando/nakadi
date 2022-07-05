package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import com.jayway.restassured.specification.RequestSpecification;
import org.apache.http.HttpStatus;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Assert;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.ItemsWrapper;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.domain.SubscriptionEventTypeStats;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.view.PartitionCountView;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.view.TimelineView;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.IntFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.text.MessageFormat.format;
import static org.springframework.http.HttpStatus.NO_CONTENT;
import static org.springframework.http.HttpStatus.OK;

public class NakadiTestUtils {

    private static final ObjectMapper MAPPER = (new JsonConfig()).jacksonObjectMapper();

    public static EventType createEventType() throws JsonProcessingException {
        final EventType eventType = buildSimpleEventType();
        createEventTypeInNakadi(eventType);
        return eventType;
    }

    public static void createEventTypeInNakadi(final EventType eventType) throws JsonProcessingException {
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types").print();
    }

    public static void updateEventTypeInNakadi(final EventType eventType) throws JsonProcessingException {
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .put("/event-types/" + eventType.getName());
    }

    public static EventType createBusinessEventTypeWithPartitions(final int partitionNum)
            throws JsonProcessingException {
        final EventTypeStatistics statistics = new EventTypeStatistics();
        statistics.setMessageSize(1);
        statistics.setMessagesPerMinute(1);
        statistics.setReadParallelism(partitionNum);
        statistics.setWriteParallelism(1);

        final EventType eventType = buildSimpleEventType();
        eventType.setCategory(EventCategory.BUSINESS);
        eventType.setEnrichmentStrategies(ImmutableList.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT));
        eventType.setPartitionStrategy(PartitionStrategy.USER_DEFINED_STRATEGY);
        eventType.setDefaultStatistic(statistics);

        createEventTypeInNakadi(eventType);
        return eventType;
    }

    public static EventType buildSimpleEventType() {
        return EventTypeTestBuilder.builder().build();
    }

    public static void publishEvent(final String eventType, final String event) {
        publishEvents(eventType, 1, (i) -> event);
    }

    public static void publishEvents(final String eventType, final int count, final IntFunction<String> generator) {
        processEvents(format("/event-types/{0}/events", eventType), count, generator);
    }

    public static void deleteEvent(final String eventType, final String event) {
        deleteEvents(eventType, 1, (i) -> event);
    }

    public static void deleteEvents(final String eventType, final int count, final IntFunction<String> generator) {
        processEvents(format("/event-types/{0}/deleted-events", eventType), count, generator);
    }

    public static void processEvents(final String path, final int count, final IntFunction<String> generator) {
        final String events = IntStream.range(0, count).mapToObj(generator).collect(Collectors.joining(","));
        given()
                .body("[" + events + "]")
                .contentType(JSON)
                .post(path);
    }

    public static void repartitionEventType(final EventType eventType, final int partitionsNumber)
            throws JsonProcessingException {
        final EventTypeStatistics defaultStatistic = eventType.getDefaultStatistic();
        defaultStatistic.setReadParallelism(partitionsNumber);
        defaultStatistic.setWriteParallelism(partitionsNumber);
        eventType.setDefaultStatistic(defaultStatistic);
        final int statusCode = given()
                .body(MAPPER.writeValueAsString(new PartitionCountView(partitionsNumber)))
                .contentType(JSON)
                .put(format("/event-types/{0}/partition-count", eventType.getName()))
                .getStatusCode();
        if (statusCode != NO_CONTENT.value()) {
            throw new RuntimeException("Failed to repartition event type");
        }
    }

    public static void createTimeline(final String eventType) {
        given()
                .body("{\"storage_id\": \"default\"}")
                .contentType(JSON)
                .post(format("/event-types/{0}/timelines", eventType))
                .then()
                .statusCode(HttpStatus.SC_CREATED);
    }

    public static List<Map> listTimelines(final String eventType) throws IOException {
        final Response response = given()
                .accept(JSON)
                .get(format("/event-types/{0}/timelines", eventType));
        final String data = response.print();
        final TypeReference<List<Map>> typeReference = new TypeReference<List<Map>>() {
        };
        return MAPPER.readValue(data, typeReference);
    }

    public static void deleteTimeline(final String eventType) throws IOException {
        final Response response = given()
                .accept(JSON)
                .get(format("/event-types/{0}/timelines", eventType));
        final String data = response.print();
        final TimelineView[] timelines = MAPPER.readerFor(TimelineView[].class).readValue(data);
        Assert.assertEquals(1, timelines.length);
        given()
                .delete(format("/event-types/{0}/timelines/{1}", eventType, timelines[0].getId().toString()))
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    public static void publishBusinessEventWithUserDefinedPartition(
            final String eventType,
            final int count,
            final IntFunction<String> fooGenerator,
            final IntFunction<String> partitionGenerator) {
        publishEvents(
                eventType,
                count, i -> {
                    final JSONObject metadata = new JSONObject();
                    metadata.put("eid", UUID.randomUUID().toString());
                    metadata.put("occurred_at", (new DateTime(DateTimeZone.UTC)).toString());
                    metadata.put("partition", partitionGenerator.apply(i));

                    final JSONObject event = new JSONObject();
                    event.put("metadata", metadata);
                    event.put("foo", fooGenerator.apply(i));
                    return event.toString();
                });
    }

    public static void publishBusinessEventsWithUserDefinedPartition(
            final String eventType,
            final Map<String, String> fooToPartition) {
        final JSONArray jsonArray = new JSONArray(
                fooToPartition.entrySet().stream().map(entry -> {
                    final JSONObject metadata = new JSONObject();
                    metadata.put("eid", UUID.randomUUID().toString());
                    metadata.put("occurred_at", (new DateTime(DateTimeZone.UTC)).toString());
                    metadata.put("partition", entry.getValue());

                    final JSONObject event = new JSONObject();
                    event.put("metadata", metadata);
                    event.put("foo", entry.getKey());
                    return event;
                }).collect(Collectors.toList()));
        given()
                .body(jsonArray.toString())
                .contentType(JSON)
                .post(format("/event-types/{0}/events", eventType));

    }

    public static Subscription createSubscriptionForEventType(final String eventType) throws IOException {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(eventType)
                .buildSubscriptionBase();
        return createSubscription(subscriptionBase);
    }

    public static Subscription createSubscriptionForEventTypeFromBegin(final String eventType) throws IOException {
        final SubscriptionBase subscriptionBase = RandomSubscriptionBuilder.builder()
                .withEventType(eventType)
                .withStartFrom(SubscriptionBase.InitialPosition.BEGIN)
                .buildSubscriptionBase();
        return createSubscription(subscriptionBase);
    }

    public static Subscription createSubscription(final SubscriptionBase subscription) throws IOException {
        return createSubscription(given(), subscription);
    }

    public static Subscription createSubscription(final RequestSpecification requestSpec,
                                                  final SubscriptionBase subscription) throws IOException {
        final Response response = requestSpec
                .body(MAPPER.writeValueAsString(subscription))
                .contentType(JSON)
                .post("/subscriptions");
        return MAPPER.readValue(response.print(), Subscription.class);
    }

    public static int commitCursors(final String subscriptionId, final List<SubscriptionCursor> cursors,
                                    final String streamId) throws JsonProcessingException {
        return commitCursors(given(), subscriptionId, cursors, streamId);
    }

    public static int commitCursors(final RequestSpecification requestSpec, final String subscriptionId,
                                    final List<SubscriptionCursor> cursors, final String streamId)
            throws JsonProcessingException {
        return requestSpec
                .body(MAPPER.writeValueAsString(new ItemsWrapper<>(cursors)))
                .contentType(JSON)
                .header("X-Nakadi-StreamId", streamId)
                .post(format("/subscriptions/{0}/cursors", subscriptionId))
                .getStatusCode();
    }

    public static Response getSubscriptionStat(final Subscription subscription)
            throws IOException {
        return given()
                .contentType(JSON)
                .get("/subscriptions/{subscription_id}/stats", subscription.getId());
    }

    public static ItemsWrapper<SubscriptionCursor> getSubscriptionCursors(final Subscription subscription)
            throws IOException {
        final Response response = given().get(format("/subscriptions/{0}/cursors", subscription.getId()));
        return MAPPER.readValue(response.print(), new TypeReference<ItemsWrapper<SubscriptionCursor>>() {
        });
    }

    public static void postEvents(final String eventTypeName, final String... events) {
        final String batch = "[" + String.join(",", events) + "]";
        given()
                .contentType(JSON)
                .body(batch)
                .when()
                .post("/event-types/" + eventTypeName + "/events")
                .then()
                .statusCode(OK.value());
    }

    public static void switchTimelineDefaultStorage(final EventType eventType) {
        given()
                .contentType(JSON)
                .body(new JSONObject().put("storage_id", "default").toString())
                .post("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_CREATED);
    }

    public static EventType getEventType(final String name) throws IOException {
        return MAPPER.readValue(given()
                .header("accept", "application/json")
                .get("/event-types/{name}", name)
                .getBody().asString(), EventType.class);
    }

    public static int getNumberOfAssignedStreams(final String sid) {
        final Response response = RestAssured.when().get("/subscriptions/{sid}/stats", sid).thenReturn();
        final ItemsWrapper<SubscriptionEventTypeStats> statsItems;
        try {
            statsItems = MAPPER.readValue(
                    response.print(),
                    new TypeReference<ItemsWrapper<SubscriptionEventTypeStats>>() {
                    });
        } catch (final IOException e) {
            throw new AssertionError("Failed to get stats", e);
        }
        final long assignedUniqueStreamsCount = statsItems.getItems()
                .stream()
                .flatMap(stat -> stat.getPartitions().stream())
                .filter(p -> "assigned".equals(p.getState()))
                .map(SubscriptionEventTypeStats.Partition::getStreamId)
                .distinct()
                .count();
        return (int) assignedUniqueStreamsCount;
    }

    public static void switchFeature(final Feature feature, final boolean enabled) {
        final JSONObject payload = new JSONObject();
        payload.put("feature", feature.getId());
        payload.put("enabled", enabled);
        given()
                .header("accept", "application/json")
                .contentType(JSON)
                .body(payload.toString())
                .post("/settings/features")
                .then()
                .statusCode(HttpStatus.SC_NO_CONTENT);
    }
}
