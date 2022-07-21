package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.apache.http.HttpStatus;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.hamcrest.Matchers;
import org.joda.time.DateTime;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.ResourceAuthorizationAttribute;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.model.AuthorizationAttributeQueryParser;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.view.EventOwnerSelector;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static com.jayway.restassured.RestAssured.given;
import static com.jayway.restassured.RestAssured.when;
import static com.jayway.restassured.http.ContentType.JSON;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.core.Is.is;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertEquals;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.randomTextString;
import static org.zalando.nakadi.utils.TestUtils.resourceAsString;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.publishEvent;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class EventTypeAT extends BaseAT {

    private static final String ENDPOINT = "/event-types";

    @Test
    public void whenGETThenListsEventTypes() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final String body = MAPPER.writer().writeValueAsString(eventType);

        given()
                .body(body)
                .header("accept", "application/json")
                .contentType(JSON)
                .post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        given()
                .header("accept", "application/json")
                .contentType(JSON)
                .get(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", hasItems(eventType.getName()));
    }

    @Test
    public void whenGETWithQueryEThenListsEventTypes() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final ResourceAuthorizationAttribute auth = new ResourceAuthorizationAttribute(
                "service", "stups_test" + randomTextString());
        final String owningApp = "stups_owning_app";

        eventType.setAuthorization(new ResourceAuthorization(List.of(auth), List.of(auth), List.of(auth)));
        eventType.setOwningApplication(owningApp);

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given()
                .body(body)
                .header("accept", "application/json")
                .contentType(JSON)
                .post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        given()
                .header("accept", "application/json")
                .contentType(JSON)
                .get(ENDPOINT + "?writer=" + AuthorizationAttributeQueryParser.getQuery(auth) +
                        "&owning_application=" + owningApp)
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", hasItems(eventType.getName()));
    }

    @Test
    public void whenPOSTValidEventTypeThenOk() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);
    }

    @Test
    public void rejectTooLongEventTypeNames() throws Exception {
        final String body = resourceAsString("../domain/event-type.with.too-long-name.json", this.getClass());

        given().body(body).header("accept", "application/json").contentType(JSON).when().post(ENDPOINT).then()
                .body(containsString("the length of the name must be")).statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenPUTValidEventTypeThenOK() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);

        final EventType retrievedEventType = MAPPER.readValue(given()
                        .header("accept", "application/json").get(ENDPOINT + "/" + eventType.getName())
                        .getBody().asString(),
                EventType.class);

        final String updateBody = MAPPER.writer().writeValueAsString(retrievedEventType);

        given().body(updateBody)
                .header("accept", "application/json")
                .contentType(JSON)
                .when()
                .put(ENDPOINT + "/" + eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenEventAuthSelectorCreatedThenOK() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setEventOwnerSelector(new EventOwnerSelector(EventOwnerSelector.Type.PATH, "x", "y"));

        given().body(MAPPER.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).post(ENDPOINT)
                .then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);

        final EventType retrievedEventType = MAPPER.readValue(given()
                        .header("accept", "application/json").get(ENDPOINT + "/" + eventType.getName())
                        .getBody().asString(),
                EventType.class);
        Assert.assertEquals(eventType.getEventOwnerSelector(), retrievedEventType.getEventOwnerSelector());
    }

    @Test
    public void whenEventAuthSelectorCreateWithMetadataAndValueThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setEventOwnerSelector(new EventOwnerSelector(EventOwnerSelector.Type.METADATA, "x", "y"));

        given().body(MAPPER.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenEventAuthSelectorUpdatedThenOK() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setEventOwnerSelector(null);

        given().body(MAPPER.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).post(ENDPOINT)
                .then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);

        final EventType retrievedEventType = MAPPER.readValue(given()
                        .header("accept", "application/json").get(ENDPOINT + "/" + eventType.getName())
                        .getBody().asString(),
                EventType.class);
        Assert.assertNull(retrievedEventType.getEventOwnerSelector());

        retrievedEventType.setEventOwnerSelector(new EventOwnerSelector(EventOwnerSelector.Type.PATH, "x", "y"));
        final String updateBody = MAPPER.writer().writeValueAsString(retrievedEventType);

        given().body(updateBody)
                .header("accept", "application/json")
                .contentType(JSON)
                .when()
                .put(ENDPOINT + "/" + eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_OK);

        final EventType updatedEventType = MAPPER.readValue(given()
                        .header("accept", "application/json").get(ENDPOINT + "/" + eventType.getName())
                        .getBody().asString(),
                EventType.class);

        Assert.assertEquals(retrievedEventType.getEventOwnerSelector(), updatedEventType.getEventOwnerSelector());
    }

    @Test
    public void whenDELETEEventTypeThenOK() throws JsonProcessingException, NoSuchEventTypeException {

        // ARRANGE //
        final EventType eventType = buildDefaultEventType();
        postEventType(eventType);
        final List<String> topics = getTopicsForEventType(eventType.getName());

        // ACT //
        deleteEventTypeAndOK(eventType);

        // ASSERT //
        checkEventTypeIsDeleted(eventType, topics);
    }

    @Test
    public void whenDELETEEventTypeWithSeveralTimelinesThenOK()
            throws JsonProcessingException, NoSuchEventTypeException {
        final EventType eventType = buildDefaultEventType();
        postEventType(eventType);
        postTimeline(eventType);
        postTimeline(eventType);
        postTimeline(eventType);
        final List<String> topics = getTopicsForEventType(eventType.getName());

        // ACT //
        deleteEventTypeAndOK(eventType);

        // ASSERT //
        checkEventTypeIsDeleted(eventType, topics);
    }

    @Test
    public void whenDELETEEventTypeWithOneTimelineThenOK() throws JsonProcessingException, NoSuchEventTypeException {
        final EventType eventType = buildDefaultEventType();
        postEventType(eventType);
        postTimeline(eventType);
        final List<String> topics = getTopicsForEventType(eventType.getName());

        // ACT //
        deleteEventTypeAndOK(eventType);

        // ASSERT //
        checkEventTypeIsDeleted(eventType, topics);
    }

    @Test
    public void whenUpdatePartitioningStrategyFromRandomThenOK() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final String bodyRandom = MAPPER.writer().writeValueAsString(eventType);

        given().body(bodyRandom).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        eventType.setPartitionStrategy(PartitionStrategy.HASH_STRATEGY);
        eventType.setPartitionKeyFields(Collections.singletonList("foo"));
        final String bodyUserDefined = MAPPER.writer().writeValueAsString(eventType);

        given().body(bodyUserDefined).header("accept", "application/json").contentType(JSON)
                .put(ENDPOINT + "/" + eventType.getName()).then().statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenUpdatePartitioningStrategyToNonExistingStrategyThen422() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final String bodyRandom = MAPPER.writer().writeValueAsString(eventType);

        given().body(bodyRandom).header("accept", "application/json").contentType(JSON).post(ENDPOINT);

        eventType.setPartitionStrategy("random1");
        final String bodyUserDefined = MAPPER.writer().writeValueAsString(eventType);

        given().body(bodyUserDefined).header("accept", "application/json").contentType(JSON)
                .put(ENDPOINT + "/" + eventType.getName()).then().statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenUpdateRetentionTimeThenUpdateInKafkaAndDB() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        IntStream.range(0, 15).forEach(x -> publishEvent(eventType.getName(), "{\"foo\":\"bar\"}"));
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);
        NakadiTestUtils.switchTimelineDefaultStorage(eventType);

        List<Map> timelines = NakadiTestUtils.listTimelines(eventType.getName());
        final String cleanupTimeBeforeUpdate = (String) timelines.get(0).get("cleaned_up_at");

        final Long defaultRetentionTime = 172800000L;
        assertRetentionTime(defaultRetentionTime, eventType.getName());

        final Long newRetentionTime = 345600000L;
        eventType.getOptions().setRetentionTime(newRetentionTime);
        final String updateBody = MAPPER.writer().writeValueAsString(eventType);
        given().body(updateBody)
                .header("accept", "application/json")
                .contentType(JSON)
                .put(ENDPOINT + "/" + eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_OK);

        timelines = NakadiTestUtils.listTimelines(eventType.getName());
        final String cleanupTimeAfterUpdate = (String) timelines.get(0).get("cleaned_up_at");
        final long cleanupTimeDiff = DateTime.parse(cleanupTimeAfterUpdate).getMillis() -
                DateTime.parse(cleanupTimeBeforeUpdate).getMillis();
        Assert.assertThat(cleanupTimeDiff, is(newRetentionTime - defaultRetentionTime));

        assertRetentionTime(newRetentionTime, eventType.getName());
    }

    @Test
    public void whenUpdateCleanupPolicyDeleteToDeleteCompactJourney() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.DELETE)
                .enrichmentStrategies(ImmutableList.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        final String body = MAPPER.writer().writeValueAsString(eventType);
        given().body(body)
                .contentType(JSON)
                .post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        final CleanupPolicy initialCleanupPolicy = CleanupPolicy.DELETE;
        assertEquals(initialCleanupPolicy, eventType.getCleanupPolicy());

        // publish an event to the event type (without partition compaction key)
        given().body("[{\"metadata\":{" +
                        "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                        "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"}}]")
                .contentType(JSON)
                .post(ENDPOINT + "/" + eventType.getName() + "/events")
                .then()
                .statusCode(HttpStatus.SC_OK);

        // update event type with compact and delete
        final CleanupPolicy updatedPolicy = CleanupPolicy.COMPACT_AND_DELETE;
        eventType.setCleanupPolicy(updatedPolicy);
        final String updateBody = MAPPER.writer().writeValueAsString(eventType);
        given().body(updateBody)
                .header("accept", "application/json")
                .contentType(JSON)
                .put(ENDPOINT + "/" + eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_OK);

        // get event type and check that properties are set correctly (database)
        given().body(body)
                .contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("cleanup_policy", equalTo("compact_and_delete"));

        // assert topic changes
        final String topic = (String) NakadiTestUtils.listTimelines(eventType.getName()).get(0).get("topic");
        final String cleanupPolicy = KafkaTestHelper.getTopicCleanupPolicy(topic);
        assertThat(cleanupPolicy, equalTo("compact,delete"));

        final Long retentionMs = KafkaTestHelper.getTopicRetentionTime(topic);
        assertEquals(retentionMs, eventType.getOptions().getRetentionTime());

        // publish event with missing compaction key and expect 422
        given().body("[{\"metadata\":{" +
                        "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                        "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"}}]")
                .contentType(JSON)
                .post(ENDPOINT + "/" + eventType.getName() + "/events")
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test(timeout = 10000)
    public void compactedEventTypeJourney() throws IOException, ExecutionException, InterruptedException {
        // create event type with 'compact' cleanup_policy
        final EventType eventType = EventTypeTestBuilder.builder()
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(ImmutableList.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        final String body = MAPPER.writer().writeValueAsString(eventType);
        given().body(body)
                .contentType(JSON)
                .post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        // get event type and check that properties are set correctly
        given().body(body)
                .contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("cleanup_policy", equalTo("compact"));

        // assert that created topic in kafka has correct cleanup_policy
        final String topic = (String) NakadiTestUtils.listTimelines(eventType.getName()).get(0).get("topic");
        final String cleanupPolicy = KafkaTestHelper.getTopicCleanupPolicy(topic);
        assertThat(cleanupPolicy, equalTo("compact"));

        // publish event to compacted event type
        publishEvent(eventType.getName(), "{\"metadata\":{" +
                "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"," +
                "\"partition_compaction_key\":\"abc\"}}");

        // assert that key was correctly propagated to event key in kafka
        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        final KafkaConsumer<byte[], byte[]> consumer = kafkaHelper.createConsumer();
        final TopicPartition tp = new TopicPartition(topic, 0);
        consumer.assign(ImmutableList.of(tp));
        consumer.seek(tp, 0);
        final ConsumerRecords<byte[], byte[]> records = consumer.poll(5000);
        final ConsumerRecord<byte[], byte[]> record = records.iterator().next();
        assertThat(record.key(), equalTo("abc".getBytes(StandardCharsets.UTF_8)));

        // publish event with missing compaction key and expect 422
        given().body("[{\"metadata\":{" +
                        "\"occurred_at\":\"1992-08-03T10:00:00Z\"," +
                        "\"eid\":\"329ed3d2-8366-11e8-adc0-fa7ae01bbebc\"}}]")
                .contentType(JSON)
                .post(ENDPOINT + "/" + eventType.getName() + "/events")
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    @Test
    public void whenUpdateRetentionTimeWithNullValueNoChange() throws Exception {
        final EventType eventType = NakadiTestUtils.createEventType();
        final Long defaultRetentionTime = 172800000L;
        assertRetentionTime(defaultRetentionTime, eventType.getName());

        eventType.getOptions().setRetentionTime(null);
        final String updateBody = MAPPER.writer().writeValueAsString(eventType);
        given().body(updateBody)
                .header("accept", "application/json")
                .contentType(JSON)
                .put(ENDPOINT + "/" + eventType.getName())
                .then()
                .body(equalTo(""))
                .statusCode(HttpStatus.SC_OK);

        final EventType eventType1 = NakadiTestUtils.getEventType(eventType.getName());
        Assert.assertEquals(defaultRetentionTime, eventType1.getOptions().getRetentionTime());
    }

    @Test
    public void whenGetEventTypeWithRegisteredExtensionThenOk() throws Exception {
        final EventType et = NakadiTestUtils.buildSimpleEventType();
        et.setName(et.getName() + ".ps");
        NakadiTestUtils.createEventTypeInNakadi(et);
        given().get(ENDPOINT + "/" + et.getName())
                .then()
                .statusCode(HttpStatus.SC_OK);
    }

    @Test
    public void whenPOSTEventTypeWithAuthorizationThenOk() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();

        eventType.setAuthorization(new ResourceAuthorization(
                ImmutableList.of(new ResourceAuthorizationAttribute("type1", "value1")),
                ImmutableList.of(new ResourceAuthorizationAttribute("type2", "value2")),
                ImmutableList.of(new ResourceAuthorizationAttribute("type3", "value3"))));

        final String body = MAPPER.writer().writeValueAsString(eventType);
        given().body(body)
                .header("accept", "application/json")
                .contentType(JSON)
                .when()
                .post(ENDPOINT)
                .then()
                .statusCode(HttpStatus.SC_CREATED);

        when().get(String.format("%s/%s", ENDPOINT, eventType.getName()))
                .then()
                .body("authorization.admins[0].data_type", equalTo("type1"))
                .body("authorization.admins[0].value", equalTo("value1"))
                .body("authorization.readers[0].data_type", equalTo("type2"))
                .body("authorization.readers[0].value", equalTo("value2"))
                .body("authorization.writers[0].data_type", equalTo("type3"))
                .body("authorization.writers[0].value", equalTo("value3"));
    }

    @Test
    public void whenUpdateETAuthObjectThen422() throws Exception {
        final ResourceAuthorization auth = new ResourceAuthorization(
                Collections.singletonList(new ResourceAuthorizationAttribute("type1", "value1")),
                Collections.singletonList(new ResourceAuthorizationAttribute("type2", "value2")),
                Collections.singletonList(new ResourceAuthorizationAttribute("type3", "value3")));
        final EventType eventType = EventTypeTestBuilder.builder().authorization(auth).build();
        NakadiTestUtils.createEventTypeInNakadi(eventType);

        eventType.setAuthorization(null);
        given()
                .body(MAPPER.writeValueAsString(eventType))
                .contentType(JSON)
                .put("/event-types/" + eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY)
                .body(equalTo(MAPPER.writer().writeValueAsString(Problem.valueOf(UNPROCESSABLE_ENTITY,
                        "Changing authorization object to `null` is not possible due to existing one"))));
    }

    @Test
    public void whenPOSTEventTypeWithAnnotationsAndLabelsThenOk() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final Map<String, String> annotations = new HashMap<>();
        annotations.put("test.io/test-key", "test-value");
        eventType.setAnnotations(annotations);
        final Map<String, String> labels = new HashMap<>();
        labels.put("test.io/test-label-key", "test-value");
        eventType.setLabels(labels);

        final String body = MAPPER.writer().writeValueAsString(eventType);

        given().body(body).header("accept", "application/json")
                .contentType(JSON).when().post(ENDPOINT).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);


        given()
                .header("accept", "application/json")
                .contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", equalTo(eventType.getName()))
                .body("annotations", hasKey("test.io/test-key"));
    }

    @Test
    public void whenPUTEventTypeWithoutAnnotationsAndLabelsThenOriginalValuesAreKept() throws JsonProcessingException {
        final ObjectMapper objectMapper = MAPPER.copy();
        objectMapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
        final EventType eventType = buildDefaultEventType();
        eventType.getAnnotations().put("nakadi.io/annotation-key", "original-annotation");
        eventType.getLabels().put("nakadi.io/label-key", "original-label");

        given().body(objectMapper.writer().writeValueAsString(eventType))
                .header("accept", "application/json").contentType(JSON)
                .when().post(ENDPOINT).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_CREATED);

        given().header("accept", "application/json").contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName()).then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", equalTo(eventType.getName()))
                .body("annotations", hasEntry("nakadi.io/annotation-key", "original-annotation"))
                .body("labels", hasEntry("nakadi.io/label-key", "original-label"));

        eventType.setAnnotations(null);
        eventType.setLabels(null);

        given().body(objectMapper.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).when().put(ENDPOINT + "/" + eventType.getName()).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_OK);

        given().header("accept", "application/json").contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName()).then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", equalTo(eventType.getName()))
                .body("annotations", hasEntry("nakadi.io/annotation-key", "original-annotation"))
                .body("labels", hasEntry("nakadi.io/label-key", "original-label"));

        eventType.setAnnotations(new HashMap<>());
        eventType.getAnnotations().put("nakadi.io/annotation-key", "new-annotation");
        eventType.setLabels(new HashMap<>());
        eventType.getLabels().put("nakadi.io/label-key", "new-label");

        given().body(objectMapper.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).when().put(ENDPOINT + "/" + eventType.getName()).then()
                .body(equalTo("")).statusCode(HttpStatus.SC_OK);

        given().header("accept", "application/json").contentType(JSON)
                .get(ENDPOINT + "/" + eventType.getName()).then()
                .statusCode(HttpStatus.SC_OK)
                .body("name", equalTo(eventType.getName()))
                .body("annotations", hasEntry("nakadi.io/annotation-key", "new-annotation"))
                .body("labels", hasEntry("nakadi.io/label-key", "new-label"));
    }

    @Test
    public void whenPOSTEventTypeWithInvalidAnnotationOrLabelThenError() throws JsonProcessingException {
        final EventType eventType = buildDefaultEventType();
        final Map<String, String> annotations = new HashMap<>();
        annotations.put("", "test-value");
        eventType.setAnnotations(annotations);
        eventType.setLabels(null);

        given().body(MAPPER.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).when().post(ENDPOINT).then()
                .body(containsString("Field \\\"annotations[]\\\" Key cannot be empty"))
                .body(containsString("Field \\\"annotations[]\\\" Key name should start and end " +
                        "with a letter or a digit"))
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);

        final Map<String, String> labels = new HashMap<>();
        labels.put("", "test-value");
        eventType.setLabels(labels);
        eventType.setAnnotations(null);

        given().body(MAPPER.writer().writeValueAsString(eventType))
                .header("accept", "application/json")
                .contentType(JSON).when().post(ENDPOINT).then()
                .body(containsString("Field \\\"labels[]\\\" Key cannot be empty"))
                .body(containsString("Field \\\"labels[]\\\" Key name should start and end " +
                        "with a letter or a digit"))
                .statusCode(HttpStatus.SC_UNPROCESSABLE_ENTITY);
    }

    private void assertRetentionTime(final Long checkingRetentionTime, final String etName) throws IOException {
        final EventType eventType = NakadiTestUtils.getEventType(etName);
        Assert.assertEquals(checkingRetentionTime, eventType.getOptions().getRetentionTime());
        TIMELINE_REPOSITORY.listTimelinesOrdered(eventType.getName()).stream()
                .map(Timeline::getTopic)
                .forEach(topic -> waitFor(() -> {
                    try {
                        Assert.assertEquals(checkingRetentionTime,
                                KafkaTestHelper.getTopicRetentionTime(topic));
                    } catch (final Exception e) {
                        throw new RuntimeException(e);
                    }
                }));
    }

    private void postTimeline(final EventType eventType) {
        given().contentType(JSON)
                .body(new JSONObject().put("storage_id", "default").toString())
                .post("event-types/{et_name}/timelines", eventType.getName())
                .then()
                .statusCode(HttpStatus.SC_CREATED);
    }

    private void checkEventTypeIsDeleted(final EventType eventType, final List<String> topics) {
        when().get(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_NOT_FOUND);
        assertEquals(0, TIMELINE_REPOSITORY.listTimelinesOrdered(eventType.getName()).size());
        final KafkaTestHelper kafkaHelper = new KafkaTestHelper(KAFKA_URL);
        // Kafka deletes topics asynchronously, so there may be a delay
        waitFor(() -> {
            final Set<String> allTopics = kafkaHelper.createConsumer().listTopics().keySet();
            topics.forEach(topic -> assertThat(allTopics, Matchers.not(hasItem(topic))));
        }, 10000);
    }

    private void postEventType(final EventType eventType) throws JsonProcessingException {
        final String body = MAPPER.writer().writeValueAsString(eventType);
        given().body(body).header("accept", "application/json").contentType(JSON).post(ENDPOINT);
    }

    private void deleteEventTypeAndOK(final EventType eventType) {
        when().delete(String.format("%s/%s", ENDPOINT, eventType.getName())).then().statusCode(HttpStatus.SC_OK);
    }

    private List<String> getTopicsForEventType(final String eventType) throws NoSuchEventTypeException {
        final List<String> topics = TIMELINE_REPOSITORY
                .listTimelinesOrdered(eventType)
                .stream()
                .map((Timeline t) -> t.getTopic())
                .collect(Collectors.toList());
        return topics;
    }
}
