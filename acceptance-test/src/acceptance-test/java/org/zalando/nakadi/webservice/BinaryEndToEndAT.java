package org.zalando.nakadi.webservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Ignore;
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
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class BinaryEndToEndAT extends BaseAT {
    private static final String TEST_ET_NAME = "nakadi.test-2022-05-06.et";

    private static final String TEST_EVENT_WITHOUT_PAYLOAD = "BQAAAHDYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlMDctYjMxM2" +
            "I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0QnRlc3QtZXQtZm9yLWRlbGV0ZS1jbGVhbnVwLXBvbGljeQICM" +
            "AAAAAAAAAAAAA==";

    private static final String TEST_COMPACTED_EVENT_WITH_PAYLOAD = "BQAAAGnYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlMDct" +
            "YjMxM2I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0NHRlc3QtZXQtZm9yLWRlbGV0ZS1wYXlsb2FkAgIwAAAAAAAAAAAt" +
            "CFBPU1QYL2V2ZW50LXR5cGVzAAxuYWthZGkUaGFzaGVkLWFwcJIDFAItBC0t";

    private static final String TEST_COMPACTED_EVENT_WITHOUT_PAYLOAD = "BQAAAGzYldi7019IMzJmNWRhZTUtNGZjNC00Y2RhLWJlM" +
            "DctYjMxM2I1ODQ5MGFiAgZoZWsC2JXYu9NfAjECFm5ha2FkaS10ZXN0OnRlc3QtZXQtZm9yLXN1Y2Nlc3NmdWwtZGVsZXRlAgIwAAAAA" +
            "AAAAAAA";

    @Test
    public void testAvroPublishingAndJsonConsumption() throws IOException {
        final Schema schema = new Schema.Parser().parse(new DefaultResourceLoader()
                .getResource("nakadi.end2end.avsc").getInputStream());
        final var et = EventTypeTestBuilder.builder()
                .name(TEST_ET_NAME)
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
                                .setEventType(TEST_ET_NAME)
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
                .post(String.format("/event-types/%s/events", TEST_ET_NAME));
        response.print();
        response.then().statusCode(200);

        // check event is consumed and format is correct
        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(TEST_ET_NAME)
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client = TestStreamingClient.create(subscription.getId()).start();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client.getBatches().size()));
        final Map event = client.getBatches().get(0).getEvents().get(0);
        Assert.assertEquals("bar", event.get("foo"));

        final Map<String, Object> metadata = (Map<String, Object>) event.get("metadata");
        Assert.assertEquals("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028", metadata.get("eid"));
        Assert.assertEquals("1.0.0", metadata.get("version"));
        Assert.assertEquals(TEST_ET_NAME, metadata.get("event_type"));
    }

    @Test
    public void testAvroDeleteCannotWorkWhenCleanupPolicyIsDelete() throws JsonProcessingException {
        final var etName = "test-et-for-delete-cleanup-policy";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.DELETE)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_EVENT_WITHOUT_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(422);
    }

    @Test
    public void testAvroDeleteCannotWorkWithPayload() throws JsonProcessingException {
        final var etName = "test-et-for-delete-payload";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_COMPACTED_EVENT_WITH_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(207);
    }

    @Test
    @Ignore
    public void testAvroDelete() throws JsonProcessingException {
        final var etName = "test-et-for-successful-delete";
        final var et = EventTypeTestBuilder.builder()
                .name(etName)
                .category(EventCategory.BUSINESS)
                .cleanupPolicy(CleanupPolicy.COMPACT)
                .enrichmentStrategies(List.of(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT))
                .build();
        NakadiTestUtils.createEventTypeInNakadi(et);

        final byte[] body = Base64.getDecoder().decode(TEST_COMPACTED_EVENT_WITHOUT_PAYLOAD);

        final var response = given()
                .contentType("application/avro-binary; charset=utf-8")
                .body(body)
                .post(String.format("/event-types/%s/deleted-events", etName));
        response.print();
        response.then()
                .statusCode(200);
    }
}
