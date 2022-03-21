package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;

    public KafkaRecordDeserializer(final AvroSchema schemas) {
        this.decoder = new AvroDeserializerWithSequenceDecoder(schemas);
    }

    public byte[] deserialize(final byte[] eventFormat, final byte[] data) {
        if (eventFormat == null) {
            // JSON
            return data;
        }

        if (Arrays.equals(eventFormat, NakadiRecord.Format.AVRO.getFormat())) {
            try {
                return decoder.deserializeAvro(EnvelopeHolder.fromBytes(data));
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize avro event", e);
            }
        }

        throw new RuntimeException(String.format(
                "event format is not defined, provided format: `%s`",
                Arrays.toString(eventFormat)));
    }
}
