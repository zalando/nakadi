package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializerWithSequenceDecoder {

    private final LocalSchemaRegistry localSchemaRegistry;
    private final Map<String, SequenceDecoder> metadataSequenceDecoders;
    private final Map<String, SequenceDecoder> eventSequenceDecoders;

    public AvroDeserializerWithSequenceDecoder(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;

        this.metadataSequenceDecoders = new HashMap<>();
        this.eventSequenceDecoders = new HashMap<>();
    }

    public byte[] deserializeAvroToJsonBytes(final EnvelopeHolder envelope) throws RuntimeException {
        try {
            final byte metadataVersion = envelope.getMetadataVersion();

            final SequenceDecoder metadataDecoder = metadataSequenceDecoders.computeIfAbsent(
                    String.valueOf(metadataVersion),
                    (v) -> new SequenceDecoder(
                            localSchemaRegistry.getEventTypeSchema(LocalSchemaRegistry.METADATA_KEY, v)
                    )
            );

            final GenericRecord metadata = metadataDecoder.read(envelope.getMetadata());

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

    private static JSONObject getJsonWithNonNullValues(final String json) {
        final var metadataObj = new JSONObject(json);
        final var iterator = metadataObj.keys();
        while (iterator.hasNext()) {
            final var key = iterator.next();
            if (metadataObj.get(key).equals(JSONObject.NULL)) {
                iterator.remove();
            }
        }
        return metadataObj;
    }

}
