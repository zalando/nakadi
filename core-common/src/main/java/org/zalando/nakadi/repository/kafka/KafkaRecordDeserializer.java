package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroSchema schemas;
    private final AvroMapper avroMapper;
    private final ObjectMapper objectMapper;

    public KafkaRecordDeserializer(final AvroSchema schemas) {
        this.schemas = schemas;
        this.avroMapper = schemas.getAvroMapper();
        this.objectMapper = schemas.getObjectMapper();
    }

    public byte[] deserialize(final byte[] eventFormat, final byte[] data) {
        if (eventFormat == null) {
            // JSON
            return data;
        }

        if (Arrays.equals(eventFormat, NakadiRecord.Format.AVRO.getFormat())) {
            try {
                final EnvelopeHolder envelop = EnvelopeHolder.fromBytes(data);
                if (envelop.getMetadataVersion() != AvroSchema.METADATA_VERSION) {
                    throw new RuntimeException(String.format(
                            "metadata version is not supported: `%d`",
                            envelop.getMetadataVersion()));
                }
                return deserializeAvro(envelop);
            } catch (IOException e) {
                throw new RuntimeException("failed to deserialize avro event", e);
            }
        }

        throw new RuntimeException(String.format(
                "event format is not defined, provided format: `%s`",
                Arrays.toString(eventFormat)));
    }

    private byte[] deserializeAvro(final EnvelopeHolder envelop) throws RuntimeException {
        try {
            // fixme use schema registry later and cache
            final JsonParser metadataParser = avroMapper.createParser(envelop.getMetadata());
            metadataParser.setSchema(new com.fasterxml.jackson.dataformat.avro.
                    AvroSchema(schemas.getMetadataSchema()));
            final ObjectNode metadataNode = metadataParser.readValueAsTree();

            final JsonParser eventParser = avroMapper.createParser(envelop.getPayload());
            eventParser.setSchema(new com.fasterxml.jackson.dataformat.avro.
                    AvroSchema(schemas.getNakadiAccessLogSchema()));
            final ObjectNode eventNode = eventParser.readValueAsTree();
            eventNode.set("metadata", metadataNode);

            // writes as UTF-8, avoiding string creation
            return objectMapper.writeValueAsBytes(eventNode);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
