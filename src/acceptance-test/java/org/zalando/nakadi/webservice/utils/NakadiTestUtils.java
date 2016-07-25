package org.zalando.nakadi.webservice.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.jayway.restassured.response.Response;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.Cursor;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.partitioning.PartitionStrategy;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static java.text.MessageFormat.format;
import static org.zalando.nakadi.utils.TestUtils.buildEventType;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;

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
                .post("/event-types");
    }

    public static EventType createBusinessEventTypeWithPartitions(final int partitionNum) throws JsonProcessingException {
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
        return buildEventType(randomValidEventTypeName(), new JSONObject("{\"additional_properties\":true}"));
    }

    public static void publishEvent(final String eventType, final String event) {
        given()
                .body(format("[{0}]", event))
                .contentType(JSON)
                .post(format("/event-types/{0}/events", eventType));
    }

    public static void publishBusinessEventWithUserDefinedPartition(final String eventType,
                                                                    final String foo,
                                                                    final String partition) {
        final JSONObject metadata = new JSONObject();
        metadata.put("eid", UUID.randomUUID().toString());
        metadata.put("occurred_at", (new DateTime(DateTimeZone.UTC)).toString());
        metadata.put("partition", partition);

        final JSONObject event = new JSONObject();
        event.put("metadata", metadata);
        event.put("foo", foo);

        publishEvent(eventType, event.toString());
    }

    public static Subscription createSubscription(final Set<String> eventTypes) throws IOException {
        final SubscriptionBase subscription = new SubscriptionBase();
        subscription.setEventTypes(eventTypes);
        subscription.setOwningApplication("my_app");
        subscription.setStartFrom(SubscriptionBase.InitialPosition.BEGIN);
        final Response response = given()
                .body(MAPPER.writeValueAsString(subscription))
                .contentType(JSON)
                .post("/subscriptions");
        return MAPPER.readValue(response.print(), Subscription.class);
    }

    public static int commitCursors(final String subscriptionId, final List<Cursor> cursors) throws JsonProcessingException {
        return given()
                .body(MAPPER.writeValueAsString(cursors))
                .contentType(JSON)
                .put(format("/subscriptions/{0}/cursors", subscriptionId))
                .getStatusCode();
    }

}
