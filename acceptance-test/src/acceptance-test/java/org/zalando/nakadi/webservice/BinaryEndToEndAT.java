package org.zalando.nakadi.webservice;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.RandomSubscriptionBuilder;
import org.zalando.nakadi.utils.TestUtils;
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
    public void testAvroPublishingAndJsonConsumption() throws IOException {

        final String jsonSchema = "{\"type\": \"object\", \"properties\": {\"foo\": {\"type\": \"string\"}}}";
        final var et = EventTypeTestBuilder.builder()
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

        // try send with wrong (json) version
        final PublishingBatch wrongVersionBatch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEventType(et.getName())
                                .setVersion("1.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("6f707837-4632-401e-a1fe-475749920a4c")
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build()))
                .build();

        final ByteBuffer wrongVersionBody = PublishingBatch.getEncoder().encode(wrongVersionBatch);
        given()
                .contentType("application/avro-binary")
                .body(wrongVersionBody.array())
                .post(String.format("/event-types/%s/events", et.getName()))
                .then()
                .statusCode(207);

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
                .post(String.format("/event-types/%s/events", et.getName()))
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
                .post(String.format("/event-types/%s/events", et.getName()))
                .then()
                .statusCode(207);

        // check event is consumed and format is correct
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(et.getName())
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client = TestStreamingClient.create(subscription.getId()).start();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client.getBatches().size()));
        final Map event = client.getBatches().get(0).getEvents().get(0);
        Assert.assertEquals("bar", event.get("foo"));

        final Map<String, Object> metadata = (Map<String, Object>) event.get("metadata");
        Assert.assertEquals("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028", metadata.get("eid"));
        Assert.assertEquals("2.0.0", metadata.get("version"));
        Assert.assertEquals(et.getName(), metadata.get("event_type"));
    }
}
