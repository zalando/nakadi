package org.zalando.nakadi.service.publishing;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@Service
public class NakadiRecordMapper {

    private final LocalSchemaRegistry localSchemaRegistry;

    public NakadiRecordMapper(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;

    }

    public List<NakadiRecord> fromBytesBatch(final byte[] batch) throws IOException {
        final List<NakadiRecord> records = new LinkedList<>();
        final ByteBuffer tmp = ByteBuffer.wrap(batch);
        while (tmp.hasRemaining()) {
            final int recordStart = tmp.position();
            final byte metadataVersion = tmp.get();
            final int metadataLength = tmp.getInt();
            final byte[] metadata = new byte[metadataLength];
            tmp.get(metadata);

            final int payloadLength = tmp.getInt();
            final byte[] payload = new byte[payloadLength];
            tmp.position(recordStart + 1 + 4 + metadataLength + 4);
            tmp.get(payload);

            // fixme use version as byte in local registry
            final Schema metadataAvroSchema = localSchemaRegistry.getEventTypeSchema(
                    LocalSchemaRegistry.METADATA_KEY, String.valueOf(metadataVersion));
            final NakadiAvroMetadata nakadiAvroMetadata = new NakadiAvroMetadata(
                    metadataVersion, metadataAvroSchema, metadata);

            records.add(new NakadiRecord()
                    .setMetadata(nakadiAvroMetadata)
                    .setPayload(payload));
        }

        return records;
    }

    public NakadiRecord fromAvroGenericRecord(final NakadiAvroMetadata metadata,
                                              final GenericRecord event) throws IOException {

        final var payloadOutputStream = new ByteArrayOutputStream();
        final var eventWriter = new GenericDatumWriter(event.getSchema());
        eventWriter.write(event, EncoderFactory.get()
                .directBinaryEncoder(payloadOutputStream, null));

        return new NakadiRecord()
                .setMetadata(metadata)
                .setEventKey(null) // fixme remove it once event key implemented
                .setPayload(payloadOutputStream.toByteArray())
                .setFormat(NakadiRecord.Format.AVRO.getFormat());
    }

}
