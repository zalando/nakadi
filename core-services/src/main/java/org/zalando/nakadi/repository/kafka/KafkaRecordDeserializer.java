package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;
    private final NakadiRecordMapper nakadiRecordMapper;

    public KafkaRecordDeserializer(final NakadiRecordMapper nakadiRecordMapper,
                                   final LocalSchemaRegistry localSchemaRegistry) {
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.decoder = new AvroDeserializerWithSequenceDecoder(localSchemaRegistry);
    }

    public byte[] deserializeToJsonBytes(final byte[] eventFormat, final byte[] data) {
        if (eventFormat == null) {
            final int formatLength = NakadiRecordMapper.Format.AVRO.getFormat().length - 1;
            if (Arrays.equals(data, 0, formatLength,
                    NakadiRecordMapper.Format.AVRO.getFormat(), 0, formatLength)) {
                final ByteArrayInputStream bais = new ByteArrayInputStream(data, 2, data.length - 1);
                final EnvelopeV0 envelope = nakadiRecordMapper.fromBytesEnvelope(bais, String.valueOf(data[1]));
                return deserializeToJsonBytes(envelope);
            }

            return data;
        }

        if (Arrays.equals(eventFormat, NakadiRecordMapper.Format.AVRO.getFormat())) {
            try {
                return decoder.deserializeAvroToJsonBytes(EnvelopeHolder.fromBytes(data));
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize avro event", e);
            }
        }

        throw new RuntimeException(String.format(
                "event format is not defined, provided format: `%s`",
                Arrays.toString(eventFormat)));
    }

    private byte[] deserializeToJsonBytes(final EnvelopeV0 envelope) {
        try {
            final MetadataV0 metadata = envelope.getMetadata();

            metadata.put("occurred_at", new DateTime(
                    (long) metadata.get("occurred_at"), DateTimeZone.UTC).toString());

            final var receivedAt = metadata.get("received_at");
            if (receivedAt != null) {
                metadata.put("received_at", new DateTime(
                        (long) receivedAt, DateTimeZone.UTC).toString());
            }

            final String eventType = metadata.get("event_type").toString();

            final SequenceDecoder eventDecoder = eventSequenceDecoders.computeIfAbsent(
                    metadata.get("version").toString(),
                    (v) -> new SequenceDecoder(
                            localSchemaRegistry.getEventTypeSchema(eventType, v))
            );

            final GenericRecord event = eventDecoder.read(envelope.getPayload());
            final StringBuilder sEvent = new StringBuilder(event.toString());

            final var sanitizedMetadata = getJsonWithNonNullValues(metadata.toString()).toString();
            sEvent.deleteCharAt(sEvent.length() - 1)
                    .append(", \"metadata\":")
                    .append(sanitizedMetadata).append('}');

            return sEvent.toString().getBytes(StandardCharsets.UTF_8);
        } catch (
                final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }

    }
}
