package org.zalando.nakadi.repository.kafka;

import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;

import java.io.IOException;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;
    private final AvroDeserializerWithSequenceDecoder decoder1;

    public KafkaRecordDeserializer(final SchemaProviderService schemaService,
                                   final LocalSchemaRegistry localSchemaRegistry) {
        this.decoder = new AvroDeserializerWithSequenceDecoder(schemaService, localSchemaRegistry);
    }

    public byte[] deserialize(final byte[] eventFormat, final byte[] data) {
        if (eventFormat == null) {
            final int formatLength = NakadiRecordMapper.Format.AVRO.getFormat().length - 1;
            if (Arrays.equals(data, 0, formatLength,
                    NakadiRecordMapper.Format.AVRO.getFormat(), 0, formatLength)) {
                return decoder1.deserializeAvro();
            }

            return data;
        }

        if (Arrays.equals(eventFormat, NakadiRecordMapper.Format.AVRO.getFormat())) {
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
