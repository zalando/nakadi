package org.zalando.nakadi.mapper;

import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public class NakadiRecordMapperTest {

    @Test
    public void testFromBytesBatch() throws IOException {
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("avro-schema/");
        final LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry(eventTypeRes);

        final PublishingBatch batch = PublishingBatch.newBuilder()
                .setEvents(List.of(Envelope.newBuilder()
                        .setMetadata(Metadata.newBuilder()
                                .setEid(UUID.randomUUID().toString())
                                .setOccurredAt(Instant.now())
                                .setVersion("1.0.0")
                                .setEventType("some.event.type")
                                .build())
                        .setPayload(ByteBuffer.wrap("First record for testing !!!"
                                .getBytes(StandardCharsets.UTF_8))).build()))
                .build();

        final ByteBuffer byteBuffer = PublishingBatch.getEncoder().encode(batch);
        final NakadiRecordMapper mapper = new NakadiRecordMapper(localSchemaRegistry);
        final List<NakadiRecord> nakadiRecords =
                mapper.fromBytesBatch(new ByteArrayInputStream(byteBuffer.array()));

        Assert.assertEquals(
                batch.getEvents().get(0).getMetadata().getEid(),
                nakadiRecords.get(0).getMetadata().getEid()
        );
    }

    @Test
    public void testFromBytesBatchDifferentVersions() throws IOException {
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("avro-schema/");
        final LocalSchemaRegistry localSchemaRegistry = new LocalSchemaRegistry(eventTypeRes);

        final String eventEid = "AB5D12E9-8376-4584-802C-3AFA1CA1D97C";
        final PublishingBatchV0 batch = PublishingBatchV0.newBuilder()
                .setEvents(List.of(EnvelopeV0.newBuilder()
                        .setMetadata(MetadataV0.newBuilder()
                                .setEid(eventEid)
                                .setOccurredAt(Instant.now())
                                .setVersion("1.0.0")
                                .setEventType("some.event.type")
                                .build())
                        .setPayload(ByteBuffer.wrap("First record for testing !!!"
                                .getBytes(StandardCharsets.UTF_8))).build()))
                .build();

        final ByteBuffer byteBuffer = PublishingBatchV0.getEncoder().encode(batch);
        final NakadiRecordMapper mapper = new NakadiRecordMapper(localSchemaRegistry);
        final List<NakadiRecord> nakadiRecords =
                mapper.fromBytesBatch(new ByteArrayInputStream(byteBuffer.array()));

        final NakadiMetadata metadata = nakadiRecords.get(0).getMetadata();
        Assert.assertNull(metadata.getEventOwner());
        Assert.assertEquals(eventEid, metadata.getEid());
    }

}
