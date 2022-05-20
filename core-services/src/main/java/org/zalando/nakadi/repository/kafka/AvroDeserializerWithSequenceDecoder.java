package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.service.SchemaService;
import org.zalando.nakadi.service.SchemaProviderService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class AvroDeserializerWithSequenceDecoder {

    private final SchemaProviderService schemaService;
    private final Map<String, SequenceDecoder> metadataSequenceDecoders;
    private final Map<String, SequenceDecoder> eventSequenceDecoders;

    public AvroDeserializerWithSequenceDecoder(final SchemaProviderService schemaService) {
        this.schemaService = schemaService;

        this.metadataSequenceDecoders = new HashMap<>();
        this.eventSequenceDecoders = new HashMap<>();
    }

    public byte[] deserializeAvro(final EnvelopeHolder envelope) throws RuntimeException {
        try {
            final byte metadataVersion = envelope.getMetadataVersion();

            final SequenceDecoder metadataDecoder = metadataSequenceDecoders.computeIfAbsent(
                    String.valueOf(metadataVersion),
                    (v) -> new SequenceDecoder(
                            schemaService.getAvroSchema(SchemaService.EVENT_TYPE_METADATA, v)
                    )
            );

            final GenericRecord metadata = metadataDecoder.read(envelope.getMetadata());

            metadata.put(NakadiAvroMetadata.OCCURRED_AT, new DateTime(
                    (long) metadata.get(NakadiAvroMetadata.OCCURRED_AT), DateTimeZone.UTC).toString());

            metadata.put(NakadiAvroMetadata.RECEIVED_AT, new DateTime(
                    (long) metadata.get(NakadiAvroMetadata.RECEIVED_AT), DateTimeZone.UTC).toString());

            final String eventType = metadata.get(NakadiAvroMetadata.EVENT_TYPE).toString();

            final SequenceDecoder eventDecoder = eventSequenceDecoders.computeIfAbsent(
                    metadata.get(NakadiAvroMetadata.SCHEMA_VERSION).toString(),
                    (v) -> new SequenceDecoder(
                            schemaService.getAvroSchema(eventType, v)
                    )
            );

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
