package org.zalando.nakadi.webservice;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.CleanupPolicy;
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
        final String testETName = TestUtils.randomValidEventTypeName();
        final Schema schema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        final var et = EventTypeTestBuilder.builder()
                .name(testETName)
                .category(EventCategory.BUSINESS)
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
                                .setEventType(testETName)
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
                .post(String.format("/event-types/%s/events", testETName));
        response.print();
        response.then().statusCode(200);

        // check event is consumed and format is correct
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(testETName)
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client = TestStreamingClient.create(subscription.getId()).start();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client.getBatches().size()));
        final Map event = client.getBatches().get(0).getEvents().get(0);
        Assert.assertEquals("bar", event.get("foo"));

        final Map<String, Object> metadata = (Map<String, Object>) event.get("metadata");
        Assert.assertEquals("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028", metadata.get("eid"));
        Assert.assertEquals("1.0.0", metadata.get("version"));
        Assert.assertEquals(testETName, metadata.get("event_type"));
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
