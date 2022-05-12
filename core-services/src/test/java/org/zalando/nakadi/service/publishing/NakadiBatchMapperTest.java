package org.zalando.nakadi.service.publishing;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import com.google.common.primitives.Bytes;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.lang.ArrayUtils;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.VersionedAvroSchema;
import org.zalando.nakadi.service.AvroSchema;
import org.zalando.nakadi.service.NakadiRecordMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;

public class NakadiBatchMapperTest {

    @Test
    public void testMap() throws IOException {
        final var eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        final AvroSchema avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
        final byte[] firstRecord = generateRecord(avroSchema);
        final byte[] secondRecord = generateRecord(avroSchema);
        final byte[] input = Bytes.concat(firstRecord, secondRecord);

        final NakadiRecordMapper mapper = new NakadiRecordMapper(avroSchema);
        final List<NakadiRecord> records = mapper.fromBytesBatch(input);

        Assert.assertEquals(2, records.size());

        Assert.assertNotNull(records.get(0).getMetadata());
        Assert.assertNotNull(records.get(1).getMetadata());

        Assert.assertArrayEquals(firstRecord, records.get(0).getData());
        Assert.assertArrayEquals(secondRecord, records.get(1).getData());
    }

    private byte[] generateRecord(final AvroSchema avroSchema) throws IOException {
        final VersionedAvroSchema versionedSchema =
                avroSchema.getLatestEventTypeSchemaVersion(AvroSchema.METADATA_KEY);
        final GenericRecord metadata =
                new GenericData.Record(versionedSchema.getSchema());

        final long someEqualTime = 1643290232172l;
        metadata.put("occurred_at", someEqualTime);
        metadata.put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab");
        metadata.put("flow_id", "hek");
        metadata.put("event_type", "nakadi.access.log");
        metadata.put("partition", "0");
        metadata.put("received_at", someEqualTime);
        metadata.put("version", "schemaVersion");
        metadata.put("published_by", "nakadi-test");

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final GenericDatumWriter eventWriter = new GenericDatumWriter(metadata.getSchema());
        eventWriter.write(metadata, EncoderFactory.get()
                .directBinaryEncoder(baos, null));

        final var meta = baos.toByteArray();
        final var metadataBytes = ArrayUtils.addAll(ByteBuffer.allocate(4).putInt(meta.length).array(), meta);
        final var payloadBytes = ArrayUtils.addAll(ByteBuffer.allocate(4).putInt(2).array(), new byte[]{'y', 'y'});
        return Bytes.concat(new byte[]{Byte.parseByte(versionedSchema.getVersion())}, metadataBytes, payloadBytes);
    }
}
