package de.zalando.aruha.nakadi.webservice.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.domain.SubscriptionBase;
import org.json.JSONObject;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.http.ContentType.JSON;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildEventType;
import static de.zalando.aruha.nakadi.utils.TestUtils.randomValidEventTypeName;
import static java.text.MessageFormat.format;

public class NakadiTestUtils {

    private static final ObjectMapper mapper = (new JsonConfig()).jacksonObjectMapper();

    public static EventType createEventType() throws JsonProcessingException {
        final EventType eventType = buildEventType(randomValidEventTypeName(),
                new JSONObject("{\"additional_properties\":true}"));
        given()
                .body(mapper.writeValueAsString(eventType))
                .contentType(JSON)
                .post("/event-types");
        return eventType;
    }

    public static void publishMessage(final String eventType, final String message) {
        given()
                .body(format("[{0}]", message))
                .contentType(JSON)
                .post(format("/event-types/{0}/events", eventType));
    }

    public static Subscription createSubscription(final Set<String> eventTypes) throws IOException {
        final SubscriptionBase subscription = new SubscriptionBase();
        subscription.setEventTypes(eventTypes);
        subscription.setOwningApplication("my_app");
        subscription.setStartFrom(SubscriptionBase.POSITION_BEGIN);
        Response response = given()
                .body(mapper.writeValueAsString(subscription))
                .contentType(JSON)
                .post("/subscriptions");
        return mapper.readValue(response.print(), Subscription.class);
    }

    public static int commitCursors(final String subscriptionId, final List<Cursor> cursors) throws JsonProcessingException {
        return given()
                .body(mapper.writeValueAsString(cursors))
                .contentType(JSON)
                .put(format("/subscriptions/{0}/cursors", subscriptionId))
                .getStatusCode();
    }

}
