package org.zalando.nakadi.repository.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.message.BinaryMessageDecoder;
import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.specific.SpecificData;
import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.service.SchemaProviderService;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KafkaRecordDeserializer implements RecordDeserializer {

    // https://avro.apache.org/docs/current/spec.html#single_object_encoding_spec
    private static final byte[] AVRO_V1_HEADER = new byte[]{(byte) 0xC3, (byte) 0x01};

    private static final Map<Schema, RawMessageDecoder<GenericRecord>> RAW_DECODERS = new ConcurrentHashMap<>();
    private static final Map<Schema, BinaryMessageDecoder<GenericRecord>> BINARY_DECODERS = new ConcurrentHashMap<>();

    private final SchemaProviderService schemaService;
    private final NakadiRecordMapper nakadiRecordMapper;

    public KafkaRecordDeserializer(final NakadiRecordMapper nakadiRecordMapper,
                                   final SchemaProviderService schemaService) {
        this.nakadiRecordMapper = nakadiRecordMapper;
        this.schemaService = schemaService;
    }

    public byte[] deserializeToJsonBytes(final byte[] data) {
        if (data == null) {
            return null;
        }

        if (data[0] == AVRO_V1_HEADER[0] && data[1] == AVRO_V1_HEADER[1]) {
            final Envelope envelope = nakadiRecordMapper.fromBytesEnvelope(data);
            return deserializeToJsonBytes(envelope);
        } else {
            // then it should be JSON
            return data;
        }
    }

    private byte[] deserializeToJsonBytes(final Envelope envelope) {
        try {
            final Metadata metadata = envelope.getMetadata();
            final Schema schema = schemaService.getAvroSchema(
                    metadata.getEventType(), metadata.getVersion());

            final GenericRecord event;
            if (envelope.getPayload().array()[0] == AVRO_V1_HEADER[0] &&
                    envelope.getPayload().array()[1] == AVRO_V1_HEADER[1]) {
                final BinaryMessageDecoder<GenericRecord> decoder = BINARY_DECODERS.computeIfAbsent(
                        schema, (s) -> new BinaryMessageDecoder<>(GenericData.get(), s)
                );
                event = decoder.decode(envelope.getPayload());
            } else {
                final RawMessageDecoder<GenericRecord> decoder = RAW_DECODERS.computeIfAbsent(
                        schema, (s) -> new RawMessageDecoder<>(SpecificData.get(), s)
                );
                event = decoder.decode(envelope.getPayload());
            }
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

    private JSONObject mapToJson(final Metadata metadata) {
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
