package org.zalando.nakadi.webservice;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
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

import static com.jayway.restassured.RestAssured.given;
import static org.zalando.nakadi.domain.SubscriptionBase.InitialPosition.BEGIN;
import static org.zalando.nakadi.webservice.utils.NakadiTestUtils.createSubscription;

public class BinaryEndToEndAT extends BaseAT {
    private static final String TEST_ET_NAME = "nakadi.test-2022-05-06.et";

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

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new GenericDatumWriter(schema).write(
                new GenericRecordBuilder(schema).set("foo", "bar").build(),
                EncoderFactory.get().directBinaryEncoder(baos, null));

        final PublishingBatchV0 batch = PublishingBatchV0.newBuilder()
                .setEvents(List.of(EnvelopeV0.newBuilder()
                        .setMetadata(MetadataV0.newBuilder()
                                .setEventType(TEST_ET_NAME)
                                .setVersion("1.0.0")
                                .setOccurredAt(Instant.now())
                                .setEid("CE8C9EBC-3F19-4B9D-A453-08AD2EDA6028")
                                .build())
                        .setPayload(ByteBuffer.wrap(baos.toByteArray()))
                        .build()))
                .build();

        baos = new ByteArrayOutputStream();
        new SpecificDatumWriter<>(PublishingBatchV0.SCHEMA$).write(batch,
                EncoderFactory.get().directBinaryEncoder(baos, null));

        final var response = given()
                .contentType("application/avro-binary")
                .header("X-Nakadi-Batch-Version", "0")
                .body(baos.toByteArray())
                .post(String.format("/event-types/%s/events", TEST_ET_NAME));
        response.print();
        response.then().statusCode(200);

        // check consumption

        final Subscription subscription = createSubscription(
                RandomSubscriptionBuilder.builder()
                        .withEventType(TEST_ET_NAME)
                        .withStartFrom(BEGIN)
                        .buildSubscriptionBase());
        final TestStreamingClient client = TestStreamingClient.create(subscription.getId()).start();

        TestUtils.waitFor(() -> Assert.assertEquals(1, client.getBatches().size()));
        Assert.assertEquals("bar", client.getBatches().get(0).getEvents().get(0).get("foo"));
    }
}
