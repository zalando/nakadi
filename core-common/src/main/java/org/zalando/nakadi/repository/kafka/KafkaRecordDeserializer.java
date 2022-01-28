package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroFactory;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final AvroSchema schemas;
    private final AvroFactory factory;

    public KafkaRecordDeserializer(final AvroSchema schemas) {
        this.schemas = schemas;
        this.factory = new AvroFactory();
        this.factory.setCodec(new AvroMapper());
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
            final JsonParser metadataParser = factory.createParser(envelop.getMetadata());
            metadataParser.setSchema(new com.fasterxml.jackson.dataformat.avro.
                    AvroSchema(schemas.getMetadataSchema()));
            final ObjectNode metadataNode = metadataParser.readValueAsTree();

            final JsonParser eventParser = factory.createParser(envelop.getPayload());
            eventParser.setSchema(new com.fasterxml.jackson.dataformat.avro.
                    AvroSchema(schemas.getNakadiAccessLogSchema()));
            final ObjectNode eventNode = eventParser.readValueAsTree();
            eventNode.set("metadata", metadataNode);

            return eventNode.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
