package de.zalando.aruha.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Sets;
import com.jayway.restassured.response.Header;
import com.jayway.restassured.response.Response;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTestHelper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static com.jayway.restassured.RestAssured.given;
import static java.text.MessageFormat.format;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

public class EventStreamReadingAT extends BaseAT {

    private static final String TEST_PARTITION = "0";
    private static final String DUMMY_EVENT = "Dummy";
    private static final String STREAM_ENDPOINT = createStreamEndpointUrl(TEST_TOPIC);
    private static final String SEPARATOR = "\n";

    private final ObjectMapper jsonMapper = new ObjectMapper();
    private KafkaTestHelper kafkaHelper;
    private String xNakadiCursors;
    private List<Cursor> initialCursors;
    private List<Cursor> kafkaInitialNextOffsets;

    @Before
    public void setUp() throws InterruptedException, JsonProcessingException {
        kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        initialCursors = kafkaHelper.getOffsetsToReadFromLatest(TEST_TOPIC);
        kafkaInitialNextOffsets = kafkaHelper.getNextOffsets(TEST_TOPIC);
        xNakadiCursors = jsonMapper.writeValueAsString(initialCursors);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushFewEventsAndReadThenGetEventsInStream()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, TEST_TOPIC, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        assertThat(batches, hasSize(PARTITIONS_NUM));
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
        final String expectedOffset = Long.toString(Long.parseLong(partitionCursor.getOffset()) - 1 + eventsPushed);

        // check that batch has offset, partition and events number we expect
        validateBatch(batchToCheck, TEST_PARTITION, expectedOffset, eventsPushed);
    }

    @Test(timeout = 10000)
    public void whenAcceptEncodingGzipReceiveCompressedStream()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        // ARRANGE //
        // push events to one of the partitions
        final int eventsPushed = 2;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, TEST_TOPIC, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .header(new Header("Accept-Encoding", "gzip"))
                .param("batch_limit", "5")
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");
        response.then().header("Content-Encoding", "gzip");
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenPushedAmountOfEventsMoreThanBatchSizeAndReadThenGetEventsInMultipleBatches()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        // ARRANGE //
        // push events to one of the partitions so that they don't fit into one branch
        final int batchLimit = 5;
        final int eventsPushed = 8;
        kafkaHelper.writeMultipleMessageToPartition(TEST_PARTITION, TEST_TOPIC, DUMMY_EVENT, eventsPushed);

        // ACT //
        final Response response = given()
                .header(new Header("X-nakadi-cursors", xNakadiCursors))
                .param("batch_limit", batchLimit)
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        assertThat(batches, hasSize(PARTITIONS_NUM + 1)); // for partition with events we should get 2 batches
        batches.forEach(batch -> validateBatchStructure(batch, DUMMY_EVENT));

        // find the batches where we expect to see the messages we pushed
        final List<Map<String, Object>> batchesToCheck = batches
                .stream()
                .filter(isForPartition(TEST_PARTITION))
                .collect(Collectors.toList());
        assertThat(batchesToCheck, hasSize(2));

        // calculate the offset we expect to see in this batch in a stream
        final Cursor partitionCursor = kafkaInitialNextOffsets.stream()
                .filter(cursor -> TEST_PARTITION.equals(cursor.getPartition()))
                .findFirst()
                .orElseThrow(() -> new AssertionError("Failed to find cursor for needed partition"));
        final String expectedOffset1 = Long.toString(Long.parseLong(partitionCursor.getOffset()) - 1 + batchLimit);
        final String expectedOffset2 = Long.toString(Long.parseLong(partitionCursor.getOffset()) - 1 + eventsPushed);

        // check that batches have offset, partition and events number we expect
        validateBatch(batchesToCheck.get(0), TEST_PARTITION, expectedOffset1, batchLimit);
        validateBatch(batchesToCheck.get(1), TEST_PARTITION, expectedOffset2, eventsPushed - batchLimit);
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReadFromTheEndThenLatestOffsetsInStream()
            throws ExecutionException, InterruptedException, JsonProcessingException {

        // ACT //
        // just stream without X-nakadi-cursors; that should make nakadi to read from the very end
        final Response response = given()
                .param("stream_timeout", "2")
                .param("batch_flush_timeout", "2")
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        assertThat(batches, hasSize(PARTITIONS_NUM));
        batches.forEach(batch -> validateBatchStructure(batch, DUMMY_EVENT));

        // validate that the latest offsets in batches correspond to the newest offsets
        final Set<Cursor> offsets = batches
                .stream()
                .map(batch -> {
                    final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
                    return new Cursor(cursor.get("partition"), cursor.get("offset"));
                })
                .collect(Collectors.toSet());
        assertThat(offsets, equalTo(Sets.newHashSet(initialCursors)));
    }

    @Test(timeout = 10000)
    @SuppressWarnings("unchecked")
    public void whenReachKeepAliveLimitThenStreamIsClosed()
            throws ExecutionException, InterruptedException, JsonProcessingException {
        // ACT //
        final int keepAliveLimit = 3;
        final Response response = given()
                .param("batch_flush_timeout", "1")
                .param("stream_keep_alive_limit", keepAliveLimit)
                .when()
                .get(STREAM_ENDPOINT);

        // ASSERT //
        response.then().statusCode(HttpStatus.OK.value()).header(HttpHeaders.TRANSFER_ENCODING, "chunked");

        final String body = response.print();
        final List<Map<String, Object>> batches = deserializeBatches(body);

        // validate amount of batches and structure of each batch
        assertThat(batches, hasSize(PARTITIONS_NUM * keepAliveLimit));
        batches.forEach(batch -> validateBatchStructure(batch, null));
    }

    @Test(timeout = 5000)
    public void whenGetEventsWithUknownTopicThenTopicNotFound() {
        given()
                .when()
                .get(createStreamEndpointUrl("blah-topic"))
                .then()
                .statusCode(HttpStatus.NOT_FOUND.value())
                .and()
                .contentType(equalTo("application/problem+json;charset=UTF-8"))
                .and()
                .body("detail", equalTo("topic not found"));
    }

    @Test(timeout = 5000)
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() {
        given()
                .param("batch_limit", "10")
                .param("stream_limit", "5")
                .when()
                .get(STREAM_ENDPOINT)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(equalTo("application/problem+json;charset=UTF-8"))
                .and()
                .body("detail", equalTo("stream_limit can't be lower than batch_limit"));
    }

    @Test(timeout = 5000)
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() {
        given()
                .param("batch_timeout", "10")
                .param("stream_timeout", "5")
                .when()
                .get(STREAM_ENDPOINT)
                .then()
                .statusCode(HttpStatus.UNPROCESSABLE_ENTITY.value())
                .and()
                .contentType(equalTo("application/problem+json;charset=UTF-8"))
                .and()
                .body("detail", equalTo("stream_timeout can't be lower than batch_flush_timeout"));
    }

    @Test(timeout = 5000)
    public void whenIncorrectCursorsFormatThenBadRequest() {
        given()
                .header(new Header("X-nakadi-cursors", "this_is_definitely_not_a_json"))
                .when()
                .get(STREAM_ENDPOINT)
                .then()
                .statusCode(HttpStatus.BAD_REQUEST.value())
                .and()
                .contentType(equalTo("application/problem+json;charset=UTF-8"))
                .and()
                .body("detail", equalTo("incorrect syntax of X-nakadi-cursors header"));
    }

    @Test(timeout = 5000)
    public void whenInvalidCursorsThenPreconditionFailed() {
        given()
                .header(new Header("X-nakadi-cursors", "[{\"partition\":\"very_wrong_partition\",\"offset\":\"3\"}]"))
                .when()
                .get(STREAM_ENDPOINT)
                .then()
                .statusCode(HttpStatus.PRECONDITION_FAILED.value())
                .and()
                .contentType(equalTo("application/problem+json;charset=UTF-8"))
                .and()
                .body("detail", equalTo("non existing partition very_wrong_partition"));
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
                        return jsonMapper.<Map<String, Object>>readValue(batch,
                                new TypeReference<HashMap<String, Object>>() {
                                });
                    } catch (IOException e) {
                        e.printStackTrace();
                        fail("Could not deserialize response from streaming endpoint");
                        return null;
                    }
                })
                .collect(Collectors.toList());
    }

    @SuppressWarnings("unchecked")
    private void validateBatchStructure(final Map<String, Object> batch, final String expectedEvent) {
        assertThat(batch, hasKey("cursor"));
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");

        assertThat(cursor, hasKey("partition"));
        assertThat(cursor, hasKey("offset"));

        if (batch.containsKey("events")) {
            final List<String> events = (List<String>) batch.get("events");
            events.forEach(event -> assertThat(event, equalTo(expectedEvent)));
        }
    }

    @SuppressWarnings("unchecked")
    private void validateBatch(final Map<String, Object> batch, final String expectedPartition,
                               final String expectedOffset, final int expectedEventNum) {
        final Map<String, String> cursor = (Map<String, String>) batch.get("cursor");
        assertThat(cursor.get("partition"), equalTo(expectedPartition));
        assertThat(cursor.get("offset"), equalTo(expectedOffset));

        if (batch.containsKey("events")) {
            final List<String> events = (List<String>) batch.get("events");
            assertThat(events.size(), equalTo(expectedEventNum));
        }
        else {
            assertThat(0, equalTo(expectedEventNum));
        }
    }

}
