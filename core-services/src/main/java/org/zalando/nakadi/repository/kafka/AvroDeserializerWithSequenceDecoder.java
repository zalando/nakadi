package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializerWithSequenceDecoder {

    private final AvroSchema schemas;
    private final Map<String, SequenceDecoder> metadataSequenceDecoders;
    private final Map<String, SequenceDecoder> eventSequenceDecoders;

    public AvroDeserializerWithSequenceDecoder(final AvroSchema schemas) {
        this.schemas = schemas;

        this.metadataSequenceDecoders = new HashMap<>();
        this.eventSequenceDecoders = new HashMap<>();
    }

    public byte[] deserializeAvro(final EnvelopeHolder envelope) throws RuntimeException {
        try {
            final byte metadataVersion = envelope.getMetadataVersion();

            final SequenceDecoder metadataDecoder = metadataSequenceDecoders.computeIfAbsent(
                    String.valueOf(metadataVersion),
                    (v) -> new SequenceDecoder(schemas.getEventTypeSchema(AvroSchema.METADATA_KEY, v)));

            final GenericRecord metadata = metadataDecoder.read(envelope.getMetadata());

            metadata.put(NakadiAvroMetadata.OCCURRED_AT, new DateTime(
                    (long) metadata.get(NakadiAvroMetadata.OCCURRED_AT), DateTimeZone.UTC).toString());

            metadata.put(NakadiAvroMetadata.RECEIVED_AT, new DateTime(
                    (long) metadata.get(NakadiAvroMetadata.RECEIVED_AT), DateTimeZone.UTC).toString());

            final String eventType = metadata.get(NakadiAvroMetadata.EVENT_TYPE).toString();

            final String schemaVersionField =
                    metadataVersion < 4 ? "schema_version" : NakadiAvroMetadata.SCHEMA_VERSION;

            final SequenceDecoder eventDecoder = eventSequenceDecoders.computeIfAbsent(
                    metadata.get(schemaVersionField).toString(),
                    (v) -> new SequenceDecoder(schemas.getEventTypeSchema(eventType, v)));

            final GenericRecord event = eventDecoder.read(envelope.getPayload());
            final StringBuilder sEvent = new StringBuilder(event.toString());

            final var sanitizedMetadata = getJsonWithNonNullValues(metadata.toString()).toString();
            sEvent.deleteCharAt(sEvent.length() - 1)
                    .append(", \"metadata\":")
                    .append(sanitizedMetadata).append('}');

            return sEvent.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
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
