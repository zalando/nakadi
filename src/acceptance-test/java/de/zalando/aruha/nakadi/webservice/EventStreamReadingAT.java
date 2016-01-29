package de.zalando.aruha.nakadi.webservice;

import static java.text.MessageFormat.format;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;

import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

import static com.jayway.restassured.RestAssured.given;

import java.io.IOException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.http.HttpStatus;

import org.hamcrest.collection.IsCollectionWithSize;

import org.junit.BeforeClass;
import org.junit.Test;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.collect.ImmutableMap;

import com.jayway.restassured.response.Response;

import de.zalando.aruha.nakadi.webservice.utils.TestHelper;

public class EventStreamReadingAT extends BaseAT {

    private static final String TOPIC = "test-topic";
    private static final String PARTITION = "0";
    private static final String DUMMY_EVENT = "Dummy";
    private static final int EVENTS_PUSHED = 20;
    private static final String STREAM_ENDPOINT = createStreamEndpointUrl(TOPIC);
    private static final String SEPARATOR = "\n";

    private ObjectMapper jsonMapper = new ObjectMapper();
    private static String initialPartitionOffset;

    @BeforeClass
    public static void setUp() throws InterruptedException {
        final TestHelper testHelper = new TestHelper(URL);

        // grab the offsets we had initially so that we know where to start reading from
        final List<Map<String, String>> initialOffsets = testHelper.getLatestOffsets(TOPIC);
        initialPartitionOffset = testHelper.getOffsetForPartition(initialOffsets, PARTITION).orElse("0");

        // push some events so that we have something to stream
        final String event = format("\"{0}\"", DUMMY_EVENT);
        testHelper.pushEventsToPartition(TOPIC, PARTITION, event, EVENTS_PUSHED);
    }

    @Test(timeout = 15000)
    public void whenGetSeveralEventsWhenReadingFromLatestOffsetsThenOk() {

        // ACT //
        final Response response = given()
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.SC_OK);

        validateStreamResponse(response.print(), 1, EVENTS_PUSHED, DUMMY_EVENT);
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithUknownTopicThenTopicNotFound() {

        // ACT //
        given()
                .param("batch_limit", "5")
                .param("stream_timeout", "1")
                .when()
                .get(createStreamEndpointUrl("blah-topic"))
        // ASSERT //
                .then()
                .statusCode(HttpStatus.SC_NOT_FOUND)
                .and()
                .body("message", equalTo("topic not found"));
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithUknownPartitionThenPartitionNotFound() {

        // ACT //
        given().param("start_from", initialPartitionOffset).param("batch_limit", "5").param("stream_timeout", "1")
               .when().get(createStreamEndpointUrl(TOPIC, "9999"))

               // ASSERT //
               .then().statusCode(HttpStatus.SC_NOT_FOUND).body("message", equalTo("partition not found"));
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithIncorrectOffsetThenBadRequest() {

        // ACT //
        given().param("start_from", "-123").param("batch_limit", "5").param("stream_timeout", "1").when()
               .get(STREAM_ENDPOINT)

               // ASSERT //
               .then().statusCode(HttpStatus.SC_BAD_REQUEST).body("message", equalTo("start_from is invalid"));
    }

    private static String createStreamEndpointUrl(final String eventType) {
        return format("/event-types/{0}/events", eventType);
    }

    private void validateStreamResponse(final String body, final int batchesCount, final int eventsInBatch,
            final String event) {

        // deserialize the response body
        final List<Map<String, Object>> batches = Arrays.stream(body.split(SEPARATOR + SEPARATOR))
                                                        .flatMap(multiBatch ->
                                                                Arrays.stream(multiBatch.split(SEPARATOR)))
                                                        .map(batch -> {
                                                            try {
                                                                return jsonMapper.<Map<String, Object>>readValue(batch,
                                                                        new TypeReference<HashMap<String, Object>>() { });
                                                            } catch (IOException e) {
                                                                e.printStackTrace();
                                                                fail("Could not deserialize stream response");
                                                                return ImmutableMap.<String, Object>of();
                                                            }
                                                        }).collect(Collectors.toList());

        // check size
        assertThat(batches, new IsCollectionWithSize<>(equalTo(batchesCount)));

        // check staructure and content of each batch
        batches.forEach(batch -> validateBatch(batch, eventsInBatch, event));
    }

    @SuppressWarnings("unchecked")
    private void validateBatch(final Map<String, Object> batch, final int eventsInBatch, final String expectedEvent) {
        assertThat(batch, hasKey("cursor"));

        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");

        assertThat(cursor, hasKey("partition"));
        assertThat(cursor.get("partition"), equalTo(PARTITION));
        assertThat(cursor, hasKey("offset"));

        assertThat(batch, hasKey("events"));

        final List<String> events = (List<String>) batch.get("events");

        assertThat(events, new IsCollectionWithSize<>(equalTo(eventsInBatch)));
        events.forEach(event -> assertThat(event, equalTo(expectedEvent)));
    }

}
