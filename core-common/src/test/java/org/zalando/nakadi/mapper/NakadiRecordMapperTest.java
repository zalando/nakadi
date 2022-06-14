package org.zalando.nakadi.mapper;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.Schema;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.specific.SpecificData;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
import org.zalando.nakadi.generated.avro.TestEnvelope;
import org.zalando.nakadi.generated.avro.TestMetadata;
import org.zalando.nakadi.generated.avro.TestPublishingBatch;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.util.AvroUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;

public class NakadiRecordMapperTest {

    @Test
    public void testFromBytesBatch() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("avro-schema/");
        final LocalSchemaRegistry localSchemaRegistry =
                new LocalSchemaRegistry(eventTypeRes);

        final PublishingBatchV0 batch = PublishingBatchV0.newBuilder()
                .setEvents(List.of(EnvelopeV0.newBuilder()
                        .setMetadata(MetadataV0.newBuilder()
                                .setEid(UUID.randomUUID().toString())
                                .setOccurredAt(Instant.now())
                                .setVersion("1.0.0")
                                .setEventType("some.event.type")
                                .build())
                        .setPayload(ByteBuffer.wrap("First record for testing !!!"
                                .getBytes(StandardCharsets.UTF_8))).build()))
                .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new SpecificData().createDatumWriter(PublishingBatchV0.SCHEMA$)
                .write(batch, EncoderFactory.get().directBinaryEncoder(baos, null));

        final NakadiRecordMapper mapper = new NakadiRecordMapper(localSchemaRegistry);
        final List<NakadiRecord> nakadiRecords =
                mapper.fromBytesBatch(new ByteArrayInputStream(baos.toByteArray()), Byte.valueOf("0"));

        Assert.assertEquals(
                batch.getEvents().get(0).getMetadata().getEid(),
                nakadiRecords.get(0).getMetadata().getEid()
        );
    }

    @Test
    public void testFromBytesBatchDifferentVersions() throws IOException {
        final Resource eventTypeRes = new DefaultResourceLoader()
                .getResource("schemas/batch.publishing.avsc");
        final Schema schema = AvroUtils.getParsedSchema(eventTypeRes.getInputStream());
        final LocalSchemaRegistry localSchemaRegistry =
                new LocalSchemaRegistry(Map.of(LocalSchemaRegistry.BATCH_PUBLISHING_KEY,
                        new TreeMap<>(Map.of("0", schema))));

        final TestPublishingBatch batch = TestPublishingBatch.newBuilder()
                .setEvents(List.of(TestEnvelope.newBuilder()
                        .setMetadata(TestMetadata.newBuilder()
                                .setEid(UUID.randomUUID().toString())
                                .setOccurredAt(Instant.now())
                                .setVersion("1.0.0")
                                .setEventType("some.event.type")
                                .build())
                        .setPayload(ByteBuffer.wrap("First record for testing !!!"
                                .getBytes(StandardCharsets.UTF_8))).build()))
                .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        new SpecificData().createDatumWriter(TestPublishingBatch.SCHEMA$)
                .write(batch, EncoderFactory.get().directBinaryEncoder(baos, null));

        final NakadiRecordMapper mapper = new NakadiRecordMapper(localSchemaRegistry);
        final List<NakadiRecord> nakadiRecords =
                mapper.fromBytesBatch(new ByteArrayInputStream(baos.toByteArray()), Byte.valueOf("0"));

        Assert.assertNull(nakadiRecords.get(0).getMetadata().getEventOwner());
    }

}
