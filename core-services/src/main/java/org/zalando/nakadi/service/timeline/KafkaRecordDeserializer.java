package org.zalando.nakadi.service.timeline;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.repository.db.SchemaRepository;
import org.zalando.nakadi.repository.kafka.RecordDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final Map<String, GenericDatumReader> genericDatumReaders;
    private final Map<String, EventTypeSchema> eventTypeSchemas;
    private final SchemaRepository schemaRepository;
    private BinaryDecoder binaryDecoder;

    public KafkaRecordDeserializer(final SchemaRepository schemaRepository) {
        this.schemaRepository = schemaRepository;
        this.genericDatumReaders = new HashMap<>();
        this.eventTypeSchemas = new HashMap<>();
    }

    public byte[] deserialize(final ConsumerRecord<byte[], byte[]> record) {
        final Header schemaTypeHeader = record.headers().lastHeader(NakadiRecord.HEADER_SCHEMA_TYPE);
        final byte[] event;
        if (schemaTypeHeader != null &&
                Arrays.equals(schemaTypeHeader.value(), NakadiRecord.SCHEMA_TYPE_AVRO)) {
            event = deserializeAvro(record);
        } else {
            event = record.value();
        }

        return event;
    }

    private byte[] deserializeAvro(final ConsumerRecord<byte[], byte[]> record)
            throws RuntimeException {
        final byte[] eventTypeBytes = record.headers()
                .lastHeader(NakadiRecord.HEADER_EVENT_TYPE).value();
        final byte[] schemaVersionBytes = record.headers()
                .lastHeader(NakadiRecord.HEADER_SCHEMA_VERSION).value();
        try {
            final String eventType = new String(eventTypeBytes);
            final String schemaVersion = new String(schemaVersionBytes);
            final String cacheKey = String.format("%s/%s", eventType, schemaVersion);
            final EventTypeSchema schema = eventTypeSchemas.getOrDefault(cacheKey,
                    schemaRepository.getSchemaVersion(eventType, schemaVersion));
            final GenericDatumReader genericDatumReader = genericDatumReaders
                    .getOrDefault(cacheKey, new GenericDatumReader(
                            new Schema.Parser().parse(schema.getSchema())));
            if (binaryDecoder == null) {
                binaryDecoder = DecoderFactory.get()
                        .binaryDecoder(record.value(), null);
            }

            final GenericRecord genericRecord = (GenericRecord) genericDatumReader
                    .read(null, DecoderFactory.get()
                            .binaryDecoder(record.value(), binaryDecoder));
            return genericRecord.toString().getBytes(StandardCharsets.UTF_8);
        } catch (final IOException io) {
            throw new RuntimeException("failed to deserialize avro event", io);
        }
    }

}
