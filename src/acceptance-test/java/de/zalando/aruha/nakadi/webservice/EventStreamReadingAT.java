package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.webservice.utils.TestHelper;
import org.apache.http.HttpStatus;
import org.hamcrest.collection.IsCollectionWithSize;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static java.text.MessageFormat.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EventStreamReadingAT {

    private static final int PORT = 8080;
    private static final String TOPIC = "test-topic";
    private static final String PARTITION = "0";
    private static final String STREAM_ENDPOINT = format("/topics/{0}/partitions/{1}/events", TOPIC, PARTITION);
    private static final String SEPARATOR = "\n";

    private TestHelper testHelper;
    private ObjectMapper jsonMapper = new ObjectMapper();
    private List<Map<String, String>> initialOffsets;

    @Before
    public void setUp() {
        RestAssured.port = PORT;
        testHelper = new TestHelper("http://localhost:" + PORT);

        // grab the offset we had initially so that we know where to start reading from
        initialOffsets = testHelper.getLatestOffsets(TOPIC);

        // push some events so that we have something to stream
        testHelper.pushEventsToPartition(TOPIC, PARTITION, "\"Dummy\"", 25);
    }

    @Test(timeout = 10000)
    public void whenGetSingleBatchThenOk() {
        // ACT //
        final String startOffset = testHelper.getOffsetForPartition(initialOffsets, PARTITION).orElse("0");
        final Response response = given()
                .param("start_from", startOffset)
                .param("batch_limit", "100")
                .param("stream_timeout", "1")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.SC_OK);
        assertStreamResponse(response.print(), 1, 25);
    }

    @SuppressWarnings("unchecked")
    private void assertStreamResponse(final String body, final int batchesCount, final int eventsInBatch) {
        // deserialize the response body
        final List<Map<String, Object>> batches = Arrays
                .stream(body.split(SEPARATOR + SEPARATOR))
                .flatMap(multiBatch -> Arrays.stream(multiBatch.split(SEPARATOR)))
                .map(batch -> {
                    try {
                        return jsonMapper.<Map<String, Object>>readValue(batch, new TypeReference<HashMap<String, Object>>() {
                        });
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Could not deserialize stream response");
                        return ImmutableMap.<String, Object>of();
                    }
                })
                .collect(Collectors.toList());

        // check size
        assertThat(batches, new IsCollectionWithSize<>(equalTo(batchesCount)));

        // check staructure and content of each batch
        batches.forEach(batch -> {
            assertThat(batch, hasKey("cursor"));
            final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");

            assertThat(cursor, hasKey("partition"));
            assertThat(cursor.get("partition"), equalTo(PARTITION));
            assertThat(cursor, hasKey("offset"));

            assertThat(batch, hasKey("events"));
            final List<String> events = (List<String>) batch.get("events");

            assertThat(events, new IsCollectionWithSize<>(equalTo(eventsInBatch)));
            events.forEach(event -> assertThat(event, equalTo("Dummy")));
        });
    }

}
