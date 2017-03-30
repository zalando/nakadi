package org.zalando.nakadi.webservice.timelines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
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
    private static String[] cursorsDuringPublish;

    @BeforeClass
    public static void setupEventTypeWithEvents() throws JsonProcessingException, InterruptedException {
        eventType = createEventType();
        final CountDownLatch started = new CountDownLatch(1);
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimeCursors = new AtomicReference<>();
        new Thread(() -> {
            started.countDown();
            try {
                // Suppose that everything will take less then 30 seconds
                inTimeCursors.set(readCursors("BEGIN", 8, 30));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                finished.countDown();
            }
        }).start();
        started.await();
        IntStream.range(0, 2).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        createTimeline(eventType.getName());
        IntStream.range(0, 2).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        createTimeline(eventType.getName());
        IntStream.range(0, 2).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        createTimeline(eventType.getName());
        IntStream.range(0, 2).forEach(idx -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        finished.await();
        cursorsDuringPublish = inTimeCursors.get();
    }

    @Test
    public void testInTimeCursorsCorrect() {
        Assert.assertArrayEquals(
                new String[]{
                        "000000000000000000",
                        "000000000000000001",
                        "001-0001-000000000000000002",
                        "001-0001-000000000000000003",
                        "001-0002-000000000000000000",
                        "001-0002-000000000000000001",
                        "001-0003-000000000000000000",
                        "001-0003-000000000000000001",
                },
                cursorsDuringPublish
        );
    }

    @Test
    public void testAllEventsConsumed() throws IOException {
        final String[] expected = new String[]{
                "001-0001-000000000000000000",
                "001-0001-000000000000000001",
                "001-0001-000000000000000002",
                "001-0001-000000000000000003",
                "001-0002-000000000000000000",
                "001-0002-000000000000000001",
                "001-0003-000000000000000000",
                "001-0003-000000000000000001",
        };

        // Do not test last case, because it makes no sense...
        for (int idx = -1; idx < expected.length - 1; ++idx) {
            final String[] receivedOffsets = readCursors(
                    idx == -1 ? "BEGIN" : expected[idx], expected.length - 1 - idx, 1);
            final String[] testedOffsets = Arrays.copyOfRange(expected, idx + 1, expected.length);
            Assert.assertArrayEquals(testedOffsets, receivedOffsets);
        }
    }

    @Test
    public void testConsumptionFromErroredPositionBlocked() {
        given()
                .header(new Header(
                        "X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"001-0001-000000000000000004\"}]"))
                .param("batch_limit", "1")
                .param("batch_flush_timeout", "1")
                .param("stream_limit", "5")
                .param("stream_timeout", "1")
                .when()
                .get("/event-types/" + eventType.getName() + "/events")
                .then()
                .statusCode(HttpStatus.SC_PRECONDITION_FAILED);

    }

    private static String[] readCursors(final String startOffset, final int streamLimit, final int streamTimeout)
            throws IOException {
        final Response response = given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"" + startOffset + "\"}]"))
                .param("batch_limit", "1")
                .param("batch_flush_timeout", "1")
                .param("stream_limit", streamLimit)
                .param("stream_timeout", streamTimeout)
                .when()
                .get("/event-types/" + eventType.getName() + "/events");

        response
                .then()
                .statusCode(HttpStatus.SC_OK);
        final String[] events = response.print().split("\n");
        final List<String> result = new ArrayList<>();
        for (int i = 0; i < events.length; ++i) {
            final ObjectNode batch = (ObjectNode) new ObjectMapper().readTree(events[i]);
            if (batch.get("events") == null) {
                continue;
            }
            final ObjectNode cursor = (ObjectNode) batch.get("cursor");
            result.add(cursor.get("offset").asText());
        }
        return result.toArray(new String[result.size()]);
    }

}
