package org.zalando.nakadi.webservice.timelines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import java.io.IOException;
import java.util.Arrays;
import java.util.stream.IntStream;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.webservice.BaseAT;
import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createTimeline;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;

public class TimelineConsumptionTest extends BaseAT {
    private static EventType eventType;

    @BeforeClass
    public static void setupEventTypeWithEvents() throws JsonProcessingException {
        // 1. create event-type
        eventType = createEventType();
        // 2. send 3 events to first timeline
        IntStream.range(0, 3).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        // 3. start next timeline
        createTimeline(eventType.getName()); // Actually one should call twice, because first one is the same topic
        createTimeline(eventType.getName());
        // 4. send 2 events to second timeline
        IntStream.range(0, 2).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
    }

    @Test
    public void testAllEventsConsumed() throws IOException {
        final String[] expected = new String[]{
                "001-0001-000000000000000000",
                "001-0001-000000000000000001",
                "001-0001-000000000000000002",
                "001-0002-000000000000000000",
                "001-0002-000000000000000001"
        };

        // Do not test last case, because it makes no sense...
        for (int idx = -1; idx < expected.length - 1; ++idx) {
            final String[] receivedOffsets = readCursors(
                    idx == -1 ? "BEGIN" : expected[idx], expected.length - 1 - idx);
            final String[] testedOffsets = Arrays.copyOfRange(expected, idx + 1, expected.length);
            Assert.assertArrayEquals(testedOffsets, receivedOffsets);
        }
    }

    @Test
    public void testConsumptionFromErroredPositionBlocked() {
        given()
                .header(new Header(
                        "X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"001-0001-000000000000000003\"}]"))
                .param("batch_limit", "1")
                .param("batch_flush_timeout", "1")
                .param("stream_limit", "5")
                .param("stream_timeout", "1")
                .when()
                .get("/event-types/" + eventType.getName() + "/events")
                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);

    }

    private String[] readCursors(final String startOffset, final int streamLimit) throws IOException {
        final Response response = given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"" + startOffset + "\"}]"))
                .param("batch_limit", "1")
                .param("batch_flush_timeout", "1")
                .param("stream_limit", streamLimit)
                .param("stream_timeout", "1")
                .when()
                .get("/event-types/" + eventType.getName() + "/events");

        response
                .then()
                .statusCode(HttpStatus.SC_OK);
        final String[] events = response.print().split("\n");

        for (int i = 0; i < events.length; ++i) {
            final ObjectNode cursor = (ObjectNode) new ObjectMapper().readTree(events[i]).get("cursor");
            events[i] = cursor.get("offset").asText();
        }
        return events;
    }

}
