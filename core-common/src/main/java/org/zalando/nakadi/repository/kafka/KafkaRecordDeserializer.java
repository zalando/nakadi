package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;

    public KafkaRecordDeserializer(final AvroSchema schemas, final String eventTypeName) {
        this.decoder = new AvroDeserializerWithSequenceDecoder(schemas, eventTypeName);
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
                            "Metadata version is not supported: `%d`",
                            envelop.getMetadataVersion()));
                }
                return decoder.deserializeAvro(envelop);
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize avro event", e);
            }
        }

        throw new RuntimeException(String.format(
                "event format is not defined, provided format: `%s`",
                Arrays.toString(eventFormat)));
    }
}
