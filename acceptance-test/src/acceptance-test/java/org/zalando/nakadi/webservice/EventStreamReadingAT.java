package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.Sets;
import com.jayway.restassured.RestAssured;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import javax.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.text.MessageFormat.format;
import static java.util.stream.IntStream.range;

public class EventStreamReadingAT extends BaseAT {

    private static final String TEST_PARTITION = "0";
    private static final int PARTITIONS_NUM = 8;
    private static final String DUMMY_EVENT = "Dummy";
    private static final String SEPARATOR = "\n";

    private static String streamEndpoint;
    private static String topicName;
    private static EventType eventType;

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private KafkaTestHelper kafkaHelper;
    private String xNakadiCursors;
    private List<Cursor> initialCursors;
    private List<Cursor> kafkaInitialNextOffsets;

    @BeforeClass
    public static void setupClass() throws JsonProcessingException {
        eventType = EventTypeTestBuilder.builder()
                .defaultStatistic(new EventTypeStatistics(PARTITIONS_NUM, PARTITIONS_NUM))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(eventType);
        streamEndpoint = createStreamEndpointUrl(eventType.getName());
        // expect only one timeline, because we just created event type
        topicName = BaseAT.TIMELINE_REPOSITORY.listTimelinesOrdered(eventType.getName()).get(0).getTopic();
    }

    @Before
    public void setUp() throws JsonProcessingException {
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        initialCursors = kafkaHelper.getOffsetsToReadFromLatest(topicName);
        kafkaInitialNextOffsets = kafkaHelper.getNextOffsets(topicName);
        xNakadiCursors = jsonMapper.writeValueAsString(initialCursors);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushFewEventsAndReadThenGetEventsInStream()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = readEvents();

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, Matchers.hasSize(PARTITIONS_NUM));
        batches.forEach(batch -> validateBatchStructure(batch, DUMMY_EVENT));

        // find the batch where we expect to see the messages we pushed
        final Map<String, Object> batchToCheck = batches
                .stream()
                .filter(isForPartition(TEST_PARTITION))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find a partition in a stream"));

        // calculate the offset we expect to see in this batch in a stream
        final Cursor partitionCursor = kafkaInitialNextOffsets.stream()
                .filter(cursor -> TEST_PARTITION.equals(cursor.getPartition()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find cursor for needed partition"));
        final String expectedOffset = TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 +
                eventsPushed);

        // check that batch has offset, partition and events number we expect
        validateBatch(batchToCheck, TEST_PARTITION, expectedOffset, eventsPushed);
    }

    @Test(timeout = 10000)
    public void whenAcceptEncodingGzipReceiveCompressedStream()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .header(new Header("Accept-Encoding", "gzip"))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");
        response.then().header("Content-Encoding", "gzip");
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushedAmountOfEventsMoreThanBatchSizeAndReadThenGetEventsInMultipleBatches()
            throws ExecutionException, InterruptedException {

        // ARRANGE //
        // push events to one of the partitions so that they don't fit into one branch
        final int batchLimit = 5;
        final int eventsPushed = 8;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, topicName, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", batchLimit)
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        // for partition with events we should get 2 batches
        Assert.assertThat(batches, Matchers.hasSize(PARTITIONS_NUM + 1));
        batches.forEach(batch -> validateBatchStructure(batch, DUMMY_EVENT));

        // find the batches where we expect to see the messages we pushed
        final List<Map<String, Object>> batchesToCheck = batches
                .stream()
                .filter(isForPartition(TEST_PARTITION))
                .collect(Collectors.toList());
        Assert.assertThat(batchesToCheck, Matchers.hasSize(2));

        // calculate the offset we expect to see in this batch in a stream
        final Cursor partitionCursor = kafkaInitialNextOffsets.stream()
                .filter(cursor -> TEST_PARTITION.equals(cursor.getPartition()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find cursor for needed partition"));
        final String expectedOffset1 =
                TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 + batchLimit);
        final String expectedOffset2 =
                TestUtils.toTimelineOffset(Long.parseLong(partitionCursor.getOffset()) - 1 + eventsPushed);

        // check that batches have offset, partition and events number we expect
        validateBatch(batchesToCheck.get(0), TEST_PARTITION, expectedOffset1, batchLimit);
        validateBatch(batchesToCheck.get(1), TEST_PARTITION, expectedOffset2, eventsPushed - batchLimit);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReadFromTheEndThenLatestOffsetsInStream() {

        // ACT //
        // just stream without X-nakadi-cursors; that should make nakadi to read from the very end
        final Response response = RestAssured.given()
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, Matchers.hasSize(PARTITIONS_NUM));
        batches.forEach(batch -> validateBatchStructure(batch, DUMMY_EVENT));

        // validate that the latest offsets in batches correspond to the newest offsets
        final Set<Cursor> offsets = batches
                .stream()
                .map(batch -> {
                    final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
                    return new Cursor(cursor.get("partition"), cursor.get("offset"));
                })
                .collect(Collectors.toSet());
        Assert.assertThat(offsets, Matchers.equalTo(Sets.newHashSet(initialCursors)));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReachKeepAliveLimitThenStreamIsClosed() {
        // ACT //
        final int keepAliveLimit = 3;
        final Response response = RestAssured.given()
                .param("batch_flush_timeout", "1")
                .param("stream_keep_alive_limit", keepAliveLimit)
                .when()
                .get(streamEndpoint);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        Assert.assertThat(batches, Matchers.hasSize(PARTITIONS_NUM * keepAliveLimit));
        batches.forEach(batch -> validateBatchStructure(batch, null));
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithUnknownTopicThenTopicNotFound() {
        RestAssured.given()
                .when()
                .get(createStreamEndpointUrl("blah-topic"))
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("topic not found"));
    }

    @Test(timeout = 5000)
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() {
        RestAssured.given()
                .param("batch_limit", "10")
                .param("stream_limit", "5")
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("stream_limit can't be lower than batch_limit"));
    }

    @Test(timeout = 5000)
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() {
        RestAssured.given()
                .param("batch_timeout", "10")
                .param("stream_timeout", "5")
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("stream_timeout can't be lower than batch_flush_timeout"));
    }

    @Test(timeout = 5000)
    public void whenIncorrectCursorsFormatThenBadRequest() {
        RestAssured.given()
                .header(new Header("X-nakadi-cursors", "this_is_definitely_not_a_json"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("incorrect syntax of X-nakadi-cursors header"));
    }

    @Test(timeout = 5000)
    public void whenInvalidCursorsThenPreconditionFailed() {
        RestAssured.given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\":\"very_wrong_partition\",\"offset\":\"3\"}]"))
                .when()
                .get(streamEndpoint)
                .then()
                .statusCode(HttpStatus.PRECONDITION_FAILED.value())
                .and()
                .contentType(Matchers.equalTo("application/problem+json"))
                .and()
                .body("detail", Matchers.equalTo("non existing partition very_wrong_partition"));
    }

    @Test(timeout = 10000)
    public void whenReadEventsForBlockedConsumerThen403() throws Exception {
        readEvents()
                .then()
                .statusCode(HttpStatus.OK.value());

        SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        try {
            TestUtils.waitFor(() -> readEvents()
                    .then()
                    .statusCode(403)
                    .body("detail", Matchers.equalTo("Application or event type is blocked")), 1000, 200);
        } finally {
            SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        }

        readEvents()
                .then()
                .statusCode(HttpStatus.OK.value());
    }

    private Response readEvents() {
        return RestAssured.given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(streamEndpoint);
    }

    @Ignore
    @Test(timeout = 10000)
    public void whenExceedMaxConsumersNumThen429() throws IOException, InterruptedException, ExecutionException {
        final String etName = NakadiTestUtils.createEventType().getName();

        // try to create 8 consuming connections
        final List<CompletableFuture<HttpURLConnection>> connectionFutures = range(0, 8)
                .mapToObj(x -> createConsumingConnection(etName))
                .collect(Collectors.toList());

        Assert.assertThat("first 5 connections should be accepted",
                countStatusCode(connectionFutures, HttpStatus.OK.value()),
                Matchers.equalTo(5));

        Assert.assertThat("last 3 connections should be rejected",
                countStatusCode(connectionFutures, HttpStatus.TOO_MANY_REQUESTS.value()),
                Matchers.equalTo(3));

        // close one of open connections
        for (final CompletableFuture<HttpURLConnection> conn : connectionFutures) {
            if (conn.get().getResponseCode() == HttpStatus.OK.value()) {
                conn.get().disconnect();
                break;
            }
        }
        range(0, 15).forEach(value -> NakadiTestUtils.publishEvent(etName, "{\"foo\": \"bar\"}"));

        // wait for Nakadi to recognize thаt connection is closed
        ThreadUtils.sleep(1000);

        // try to create 3 more connections
        final List<CompletableFuture<HttpURLConnection>> moreConnectionFutures = range(0, 3)
                .mapToObj(x -> createConsumingConnection(etName))
                .collect(Collectors.toList());

        Assert.assertThat("one more connection should be accepted",
                countStatusCode(moreConnectionFutures, HttpStatus.OK.value()),
                Matchers.equalTo(1));

        Assert.assertThat("other 2 connections should be rejected as we had only one new free slot",
                countStatusCode(moreConnectionFutures, HttpStatus.TOO_MANY_REQUESTS.value()),
                Matchers.equalTo(2));
    }

    private int countStatusCode(final List<CompletableFuture<HttpURLConnection>> futures, final int statusCode) {
        return (int) futures.stream()
                .filter(future -> {
                    try {
                        return future.get(5, TimeUnit.SECONDS).getResponseCode() == statusCode;
                    } catch (Exception e) {
                        throw new AssertionError("Failed to get future result");
                    }
                })
                .count();
    }

    private CompletableFuture<HttpURLConnection> createConsumingConnection(final String etName) {
        final CompletableFuture<HttpURLConnection> future = new CompletableFuture<>();
        new Thread(() -> {
            try {
                final URL url = new URL(URL + createStreamEndpointUrl(etName));
                final HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.getResponseCode(); // wait till the response code comes
                future.complete(conn);
            } catch (IOException e) {
                throw new AssertionError();
            }
        }).start();
        return future;
    }

    @Test(timeout = 10000)
    public void whenReadEventsConsumerIsBlocked() throws Exception {
        // blocking streaming client after 3 seconds
        new Thread(() -> {
            try {
                ThreadUtils.sleep(3000);
                SettingsControllerAT.blacklist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
            } catch (final Exception e) {
                e.printStackTrace();
            }
        }).start();
        try {
            // read events from the stream until we are blocked otherwise TestTimedOutException will be thrown and test
            // is considered to be failed
            RestAssured.given()
                    .header(new Header("X-nakadi-cursors", xNakadiCursors))
                    .param("batch_limit", "1")
                    .param("stream_timeout", "60")
                    .param("batch_flush_timeout", "10")
                    .when()
                    .get(streamEndpoint);
        } finally {
            SettingsControllerAT.whitelist(eventType.getName(), BlacklistService.Type.CONSUMER_ET);
        }
    }

    @Test(timeout = 10000)
    public void whenMemoryOverflowEventsDumped() throws IOException {
        // Create event type
        final EventType loadEt = EventTypeTestBuilder.builder()
                .defaultStatistic(new EventTypeStatistics(PARTITIONS_NUM, PARTITIONS_NUM))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(loadEt);

        // Publish events to event type, that are not fitting memory
        final String evt = "{\"foo\":\"barbarbar\"}";
        final int eventCount = 2 * (10000 / evt.length());
        NakadiTestUtils.publishEvents(loadEt.getName(), eventCount, i -> evt);

        // Configure streaming so it will:(more than 10s and batch_limit
        // - definitely wait for more than test timeout (10s)
        // - collect batch, which size is greater than events published to this event type
        final String url = RestAssured.baseURI + ":" + RestAssured.port + createStreamEndpointUrl(loadEt.getName())
                + "?batch_limit=" + (10 * eventCount)
                + "&stream_limit=" + (10 * eventCount)
                + "&batch_flush_timeout=11"
                + "&stream_timeout=11";
        final HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
        // Start from the begin.
        connection.setRequestProperty("X-Nakadi-Cursors",
                "[" + IntStream.range(0, PARTITIONS_NUM)
                        .mapToObj(i -> "{\"partition\": \"" + i + "\",\"offset\":\"begin\"}")
                        .collect(Collectors.joining(",")) + "]");
        Assert.assertEquals(HttpServletResponse.SC_OK, connection.getResponseCode());

        try (InputStream inputStream = connection.getInputStream()){
            final BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream, Charsets.UTF_8));

            final String line = reader.readLine();
            // If we read at least one line, than it means, that we were able to read data before test timeout reached.
            Assert.assertNotNull(line);
        }
    }


    private static String createStreamEndpointUrl(final String eventType) {
        return format("/event-types/{0}/events", eventType);
    }

    @SuppressWarnings("unchecked")
    private Predicate<Map<String, Object>> isForPartition(final String partition) {
        return batch -> {
            final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
            return partition.equals(cursor.get("partition"));
        };
    }

    private List<Map<String, Object>> deserializeBatches(final String body) {
        return Arrays
                .stream(body.split(SEPARATOR))
                .map(batch -> {
                    try {
                        return jsonMapper.readValue(batch,
                                new TypeReference<HashMap<String, Object>>() {
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                        Assert.fail("Could not deserialize response from streaming endpoint");
                        return null;
                    }
                })
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private void validateBatchStructure(final Map<String, Object> batch, final String expectedEvent) {
        Assert.assertThat(batch, Matchers.hasKey("cursor"));
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");

        Assert.assertThat(cursor, Matchers.hasKey("partition"));
        Assert.assertThat(cursor, Matchers.hasKey("offset"));

        if (batch.containsKey("events")) {
            final List<String> events = (List<String>) batch.get("events");
            events.forEach(event -> Assert.assertThat(event, Matchers.equalTo(expectedEvent)));
        }
    }

    @SuppressWarnings("unchecked")
    private void validateBatch(final Map<String, Object> batch, final String expectedPartition,
                               final String expectedOffset, final int expectedEventNum) {
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
        Assert.assertThat(cursor.get("partition"), Matchers.equalTo(expectedPartition));
        Assert.assertThat(cursor.get("offset"), Matchers.equalTo(expectedOffset));

        if (batch.containsKey("events")) {
            final List<String> events = (List<String>) batch.get("events");
            Assert.assertThat(events.size(), Matchers.equalTo(expectedEventNum));
        } else {
            Assert.assertThat(0, Matchers.equalTo(expectedEventNum));
        }
    }

}
