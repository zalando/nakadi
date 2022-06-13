package org.zalando.nakadi.repository.kafka;

import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.specific.SpecificData;
import org.json.JSONObject;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.enrichment.MetadataEnrichmentStrategy;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroDeserializerWithSequenceDecoder decoder;
    private final SchemaProviderService schemaService;
    private final NakadiRecordMapper nakadiRecordMapper;
    private final Map<String, RawMessageDecoder<GenericRecord>> decoders;

    public KafkaRecordDeserializer(final NakadiRecordMapper nakadiRecordMapper,
                                   final SchemaProviderService schemaService,
                                   final LocalSchemaRegistry localSchemaRegistry) {
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.schemaService = schemaService;
        this.decoder = new AvroDeserializerWithSequenceDecoder(localSchemaRegistry);
        this.decoders = new HashMap<>(1);
    }

    public byte[] deserializeToJsonBytes(final byte[] eventFormat, final byte[] data) {
        if (data == null) {
            return null;
        }

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

            final RawMessageDecoder<GenericRecord> decoder = decoders.computeIfAbsent(
                    metadata.getVersion(),
                    (v) -> new RawMessageDecoder<>(
                            new SpecificData(), schemaService.getAvroSchema(metadata.getEventType(), v))
            );

            final GenericRecord event = decoder.decode(envelope.getPayload());
            final StringBuilder sEvent = new StringBuilder(event.toString());

            final var sanitizedMetadata = mapToJson(metadata).toString();

            sEvent.deleteCharAt(sEvent.length() - 1)
                    .append(", \"metadata\":")
                    .append(sanitizedMetadata).append('}');

            return sEvent.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

    private JSONObject mapToJson(final MetadataV0 metadata) {
        final JSONObject metadataObj = new JSONObject(metadata.toString());
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
