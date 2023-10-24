package org.zalando.nakadi.webservice;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.http.HttpStatus;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.generated.avro.ConsumptionBatch;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.SubscriptionCursor;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class BinaryEndToEndAT extends BaseAT {

    @Test
    public void shouldPublishAvroAndConsumeJsonAndAvro() throws IOException {
        final String testETName = TestUtils.randomValidEventTypeName();
        final String jsonSchema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}}";

        final var et = EventTypeTestBuilder.builder()
                .name(testETName)
                .category(EventCategory.BUSINESS)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema(new EventTypeSchema(new EventTypeSchemaBase(EventTypeSchemaBase.Type.JSON_SCHEMA, jsonSchema),
                        "1.0.0", TestUtils.randomDate()))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final Schema avroSchema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        et.setSchema(new EventTypeSchema(
                new EventTypeSchemaBase(EventTypeSchemaBase.Type.AVRO_SCHEMA, avroSchema.toString()),
                "2.0.0", TestUtils.randomDate()));
        NakadiTestUtils.updateEventTypeInNakadi(et);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new GenericDatumWriter(avroSchema).write(
                new GenericRecordBuilder(avroSchema).set("foo", "bar").build(),
                EncoderFactory.get().directBinaryEncoder(baos, null));

        // send normal batch with avro
        final PublishingBatch batch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()

                        .setMetadata(Metadata.newBuilder()
                                .setEventType(et.getName())
                                .setVersion("2.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build()))
                .build();

        final ByteBuffer body = PublishingBatch.getEncoder().encode(batch);
        given()
                .contentType("application/avro-binary")
                .body(body.array())
                .post(String.format("/event-types/%s/events", testETName))
                .then()
                .statusCode(200);

        // try send with unknown version
        final PublishingBatch unknownVersionBatch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(et.getName())
                                .setVersion("5.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("769864fb-a0f6-4e13-a814-77bdc5633266")
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build()))
                .build();

        final ByteBuffer unknownVersionBody = PublishingBatch.getEncoder().encode(unknownVersionBatch);
        given()
                .contentType("application/avro-binary")
                .body(unknownVersionBody.array())
                .post(String.format("/event-types/%s/events", testETName))
                .then()
                .statusCode(207);

        // check event is consumed and format is correct
        final Subscription subscription1 = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(testETName)
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client1 = TestStreamingClient.create(subscription1.getId()).start();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client1.getJsonBatches().size()));
        final Map jsonEvent = client1.getJsonBatches().get(0).getEvents().get(0);
        Assert.assertEquals("bar", jsonEvent.get("foo"));

        final Map<String, Object> metadata = (Map<String, Object>) jsonEvent.get("metadata");
        Assert.assertEquals("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028", metadata.get("eid"));
        Assert.assertEquals("2.0.0", metadata.get("version"));
        Assert.assertEquals(testETName, metadata.get("event_type"));

        // check event is consumed and format is correct
        final Subscription subscription2 = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(testETName)
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client2 = TestStreamingClient.create(subscription2.getId()).startBinary();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client2.getBinaryBatches().size()));
        final ConsumptionBatch consumptionBatch = client2.getBinaryBatches().get(0);
        final Envelope binaryEvent = consumptionBatch.getEvents().get(0);
        Assert.assertEquals("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028", binaryEvent.getMetadata().getEid());

        final GenericRecord genericRecord = new GenericDatumReader<GenericRecord>(avroSchema)
                .read(null, DecoderFactory.get()
                        .binaryDecoder(binaryEvent.getPayload().array(), null));

        Assert.assertEquals("bar", genericRecord.get("foo").toString());

        final org.zalando.nakadi.generated.avro.SubscriptionCursor cursor = consumptionBatch.getCursor();
        final List<SubscriptionCursor> subscriptionCursors = List.of(new SubscriptionCursor(
                cursor.getPartition(),
                cursor.getOffset(),
                cursor.getEventType(),
                cursor.getCursorToken()));
        Assert.assertEquals(
                HttpStatus.SC_NO_CONTENT,
                NakadiTestUtils.commitCursors(
                        client2.getSubscriptionId(), subscriptionCursors, client2.getSessionId()));
    }

    @Test
    public void testAvroDeleteCannotWorkWhenCleanupPolicyIsDelete() throws IOException {
        final String etName = TestUtils.randomValidEventTypeName();
        final Schema schema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.DELETE)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema(new EventTypeSchema(new EventTypeSchemaBase(
                        EventTypeSchemaBase.Type.AVRO_SCHEMA,
                        schema.toString()), "1.0.0", TestUtils.randomDate()))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final PublishingBatch batch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(etName)
                                .setVersion("1.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .build())
                        .setPayload(ByteBuffer.wrap(new byte[0]))
                        .build()))
                .build();

        final ByteBuffer body = PublishingBatch.getEncoder().encode(batch);
        final var response = given()
                .contentType("application/avro-binary")
                .body(body.array())
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(422);
    }

    @Test
    public void testAvroDeleteCannotWorkWithPayload() throws IOException {
        final String etName = TestUtils.randomValidEventTypeName();
        final Schema schema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema(new EventTypeSchema(new EventTypeSchemaBase(
                        EventTypeSchemaBase.Type.AVRO_SCHEMA,
                        schema.toString()), "1.0.0", TestUtils.randomDate()))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new GenericDatumWriter(schema).write(
                new GenericRecordBuilder(schema).set("foo", "bar").build(),
                EncoderFactory.get().directBinaryEncoder(baos, null));

        final PublishingBatch batch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(etName)
                                .setPartitionCompactionKey("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .setVersion("1.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build()))
                .build();
        final ByteBuffer body = PublishingBatch.getEncoder().encode(batch);

        final var response = given()
                .contentType("application/avro-binary")
                .body(body.array())
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(207);
    }

    @Test
    public void testAvroDelete() throws IOException {
        final String etName = TestUtils.randomValidEventTypeName();
        final Schema schema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .schema(new EventTypeSchema(new EventTypeSchemaBase(
                        EventTypeSchemaBase.Type.AVRO_SCHEMA,
                        schema.toString()), "1.0.0", TestUtils.randomDate()))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final PublishingBatch batch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(etName)
                                .setVersion("1.0.0")
                                .setPartitionCompactionKey("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .setOccurredAt(Instant.now())
                                .setEid("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .build())
                        .setPayload(ByteBuffer.wrap(new byte[0]))
                        .build()))
                .build();
        final ByteBuffer body = PublishingBatch.getEncoder().encode(batch);

        final var response = given()
                .contentType("application/avro-binary")
                .body(body.array())
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(200);
    }
}
