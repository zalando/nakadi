package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import javax.annotation.concurrent.NotThreadSafe;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

// class is supposed to be used in a SINGLE thread!
@NotThreadSafe
public class KafkaRecordDeserializer implements RecordDeserializer {

    private final GenericDatumReader nakadiAccessLogReader;
    private final GenericDatumReader metadataDatumReader;
    private final ByteArrayOutputStream baos;
    private final AvroSchema schemas;

    public KafkaRecordDeserializer(final AvroSchema schemas) {
        this.schemas = schemas;
        this.metadataDatumReader = new GenericDatumReader(
                this.schemas.getMetadataSchema());
        this.nakadiAccessLogReader = new GenericDatumReader(
                this.schemas.getNakadiAccessLogSchema());
        this.baos = new ByteArrayOutputStream();
    }

    public byte[] deserialize(final byte[] eventFormat, final byte[] data) {
        if (eventFormat == null) {
            // JSON
            return data;
        }

        if (Arrays.equals(eventFormat, NakadiRecord.Format.AVRO.getFormat())) {
            try {
                final EnvelopeHolder envelop = EnvelopeHolder.fromBytes(data);
                if (envelop.getMetadataVersion() != AvroSchema.METADATA_VERSION) {
                    throw new RuntimeException(String.format(
                            "metadata version is not supported: `%d`",
                            envelop.getMetadataVersion()));
                }
                return deserializeAvro(envelop);
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize avro event", e);
            }
        }

        throw new RuntimeException(String.format(
                "event format is not defined, provided format: `%s`",
                Arrays.toString(eventFormat)));
    }

    private byte[] deserializeAvro(final EnvelopeHolder envelop) throws RuntimeException {
        try {
            // fixme use schema registry later and cache
            final GenericRecord metadataRecord = (GenericRecord) metadataDatumReader
                    .read(null, DecoderFactory.get()
                            .directBinaryDecoder(envelop.getMetadata(), null));

            final GenericRecord accessLogRecord = (GenericRecord) nakadiAccessLogReader
                    .read(null, DecoderFactory.get()
                            .directBinaryDecoder(envelop.getPayload(), null));
            baos.reset();
            baos.write("{\"metadata\":".getBytes(StandardCharsets.UTF_8));
            baos.write(metadataRecord.toString().getBytes(StandardCharsets.UTF_8));
            baos.write(',');
            baos.write(accessLogRecord.toString().getBytes(StandardCharsets.UTF_8));
            baos.write('}');

            return baos.toByteArray();
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
