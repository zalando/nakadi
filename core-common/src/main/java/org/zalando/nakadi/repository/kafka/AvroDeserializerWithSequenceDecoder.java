package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class AvroDeserializerWithSequenceDecoder {

    private final SequenceDecoder metadataSequenceDecoder;
    private final SequenceDecoder eventSequenceDecoder;

    public AvroDeserializerWithSequenceDecoder(final AvroSchema schemas, final String eventTypeName) {
        this.metadataSequenceDecoder = new SequenceDecoder(schemas.getMetadataSchema());
        this.eventSequenceDecoder =
                new SequenceDecoder(schemas.getLatestEventTypeSchemaVersion(eventTypeName).getValue());
    }

    public byte[] deserializeAvro(final EnvelopeHolder envelop) throws RuntimeException {
        try {
            final GenericRecord metadata = metadataSequenceDecoder.read(envelop.getMetadata());

            metadata.put("occurred_at", new DateTime(
                    (long) metadata.get("occurred_at"), DateTimeZone.UTC).toString());
            metadata.put("received_at", new DateTime(
                    (long) metadata.get("received_at"), DateTimeZone.UTC).toString());

            final GenericRecord event = eventSequenceDecoder.read(envelop.getPayload());

            final StringBuilder sEvent = new StringBuilder(event.toString());
            sEvent.deleteCharAt(sEvent.length() - 1).append(", \"metadata\":").append(metadata).append('}');

            return sEvent.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
