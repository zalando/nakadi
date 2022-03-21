package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializerWithSequenceDecoder {

    private final AvroSchema schemas;
    private final SequenceDecoder metadataSequenceDecoder;
    private final Map<String, SequenceDecoder> eventSequenceDecoders;

    public AvroDeserializerWithSequenceDecoder(final AvroSchema schemas) {
        this.schemas = schemas;

        this.metadataSequenceDecoder = new SequenceDecoder(schemas.getMetadataSchema());
        this.eventSequenceDecoders = new HashMap<>();
    }

    public byte[] deserializeAvro(final EnvelopeHolder envelop) throws RuntimeException {
        try {
            final GenericRecord metadata = metadataSequenceDecoder.read(envelop.getMetadata());

            metadata.put("occurred_at", new DateTime(
                    (long) metadata.get("occurred_at"), DateTimeZone.UTC).toString());
            metadata.put("received_at", new DateTime(
                    (long) metadata.get("received_at"), DateTimeZone.UTC).toString());

            final SequenceDecoder eventDecoder = eventSequenceDecoders.computeIfAbsent(
                    metadata.get("schema_version").toString(),
                    (v) -> new SequenceDecoder(schemas.getEventTypeSchema(metadata.get("event_type").toString(), v)));

            final GenericRecord event = eventDecoder.read(envelop.getPayload());

            final StringBuilder sEvent = new StringBuilder(event.toString());
            sEvent.deleteCharAt(sEvent.length() - 1).append(", \"metadata\":").append(metadata).append('}');

            return sEvent.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
