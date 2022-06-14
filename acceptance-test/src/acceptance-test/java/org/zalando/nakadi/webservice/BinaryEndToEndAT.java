package org.zalando.nakadi.webservice;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificDatumWriter;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.List;

import static com.jayway.restassured.RestAssured.given;

public class BinaryEndToEndAT extends BaseAT {
    private static final String TEST_ET_NAME = "nakadi.test-2022-05-06.et";
    private static final String TEST_DATA = "BAAAAGO0v/qJk2BIZjU5ZmVlNTQtMmNhYy00MTAzLWI4NTItOGMwOGRiZjhlNjEyAhJ0ZX" +
            "N0LWZsb3cAAjECEnRlc3QtdXNlcjJuYWthZGkudGVzdC0yMDIyLTA1LTA2LmV0AAAAAAAAAABBCFBPU1QYL2V2ZW50LXR5cGVzAB50ZX" +
            "N0LXVzZXItYWdlbnQMbmFrYWRpFGhhc2hlZC1hcHCSAxQCLQQtLfYBggU=";

    @Test
    public void testAvroPublishing() throws IOException {
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
                .contentType("application/avro-binary; charset=utf-8")
                .body(baos)
                .post(String.format("/event-types/%s/events", TEST_ET_NAME));
        response.print();
        response.then().statusCode(200);
        // TODO add the consumption side once schema creation is done.
    }
}
