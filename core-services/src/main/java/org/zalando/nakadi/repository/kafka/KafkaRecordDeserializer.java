package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.SchemaProviderService;

import java.io.IOException;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;

    public KafkaRecordDeserializer(final SchemaProviderService schemaService) {
        this.decoder = new AvroDeserializerWithSequenceDecoder(schemaService);
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
