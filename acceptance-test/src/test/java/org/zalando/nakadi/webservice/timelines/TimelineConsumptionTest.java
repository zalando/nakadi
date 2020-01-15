package org.zalando.nakadi.webservice.timelines;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createEventType;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createTimeline;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvents;

public class TimelineConsumptionTest {
    private static EventType eventType;
    private static String[] cursorsDuringPublish;

    @BeforeClass
    public static void setupEventTypeWithEvents() throws JsonProcessingException, InterruptedException {
        eventType = createEventType();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimeCursors = new AtomicReference<>();
        createParallelConsumer(eventType.getName(), 8, finished, inTimeCursors::set);
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        finished.await();
        cursorsDuringPublish = inTimeCursors.get();
    }

    private static void createParallelConsumer(
            final String eventTypeName,
            final int expectedEvents,
            final CountDownLatch finished,
            final Consumer<String[]> inTimeCursors) throws InterruptedException {
        final CountDownLatch started = new CountDownLatch(1);
        new Thread(() -> {
            started.countDown();
            try {
                inTimeCursors.accept(readCursors(eventTypeName, "BEGIN", expectedEvents));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                finished.countDown();
            }
        }).start();
        started.await();
    }

    @Test
    public void test2TimelinesInaRow() throws IOException, InterruptedException {
        final EventType eventType = createEventType();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimelineCursors = new AtomicReference<>();
        createParallelConsumer(eventType.getName(), 5, finished, inTimelineCursors::set);
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName()); // Still old topic
        createTimeline(eventType.getName()); // New topic
        createTimeline(eventType.getName()); // Another new topic
        publishEvents(eventType.getName(), 1, i -> "{\"foo\":\"bar\"}");
        createTimeline(eventType.getName());
        createTimeline(eventType.getName());
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        finished.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0006-000000000000000000",
                        "001-0006-000000000000000001"
                },
                inTimelineCursors.get()
        );

        final String[] receivedOffsets = readCursors(eventType.getName(), "BEGIN", 5);
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0006-000000000000000000",
                        "001-0006-000000000000000001"
                },
                receivedOffsets
        );
    }

    @Test
    public void test2TimelinesInaRowNoBegin() throws IOException, InterruptedException {
        final EventType eventType = createEventType();
        final CountDownLatch finished = new CountDownLatch(1);
        final AtomicReference<String[]> inTimelineCursors = new AtomicReference<>();
        createParallelConsumer(eventType.getName(), 2, finished, inTimelineCursors::set);
        createTimeline(eventType.getName()); // Still old topic
        createTimeline(eventType.getName()); // New topic
        createTimeline(eventType.getName()); // Another new topic
        publishEvents(eventType.getName(), 2, i -> "{\"foo\":\"bar\"}");
        finished.await();
        Assert.assertArrayEquals(
                new String[]{
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001",
                },
                inTimelineCursors.get()
        );
        final String[] receivedOffsets = readCursors(eventType.getName(), "BEGIN", 2);
        Assert.assertArrayEquals(
                new String[]{
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001",
                },
                receivedOffsets
        );
    }

    @Test
    public void testInTimeCursorsCorrect() {
        Assert.assertArrayEquals(
                new String[]{
                        "001-0001-000000000000000000",
                        "001-0001-000000000000000001",
                        "001-0002-000000000000000000",
                        "001-0002-000000000000000001",
                        "001-0003-000000000000000000",
                        "001-0003-000000000000000001",
                        "001-0004-000000000000000000",
                        "001-0004-000000000000000001"
                },
                cursorsDuringPublish
        );
    }

    @Test
    public void testAllEventsConsumed() throws IOException {
        final String[] expected = new String[]{
                "001-0001-000000000000000000",
                "001-0001-000000000000000001",
                "001-0002-000000000000000000",
                "001-0002-000000000000000001",
                "001-0003-000000000000000000",
                "001-0003-000000000000000001",
                "001-0004-000000000000000000",
                "001-0004-000000000000000001",
        };

        // Do not test last case, because it makes no sense...
        for (int idx = -1; idx < expected.length - 1; ++idx) {
            final String[] receivedOffsets = readCursors(eventType.getName(),
                    idx == -1 ? "BEGIN" : expected[idx], expected.length - 1 - idx);
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

    private static String[] readCursors(final String eventTypeName, final String startOffset, final int streamLimit)
            throws IOException {
        final Response response = given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\": \"0\", \"offset\": \"" + startOffset + "\"}]"))
                .param("batch_limit", "1")
                .param("batch_flush_timeout", "1")
                .param("stream_limit", streamLimit)
                .param("stream_timeout", 60)
                .when()
                .get("/event-types/" + eventTypeName + "/events");

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
