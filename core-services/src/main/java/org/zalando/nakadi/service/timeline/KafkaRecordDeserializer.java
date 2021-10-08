package org.zalando.nakadi.service.timeline;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.ResourceLoader;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.repository.kafka.RecordDeserializer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class KafkaRecordDeserializer implements RecordDeserializer {

    private final Map<String, GenericDatumReader> genericDatumReaders;
    private final EventTypeCache eventTypeCache;
    private final ResourceLoader resourceLoader;
    private BinaryDecoder binaryDecoder;

    public KafkaRecordDeserializer(final EventTypeCache eventTypeCache) {
        this.eventTypeCache = eventTypeCache;
        this.genericDatumReaders = new HashMap<>();
        this.resourceLoader = new DefaultResourceLoader();
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
        final byte[] eventType = record.headers()
                .lastHeader(NakadiRecord.HEADER_EVENT_TYPE).value();
        final byte[] schemaVersion = record.headers()
                .lastHeader(NakadiRecord.HEADER_SCHEMA_VERSION).value();
        try {
            final GenericDatumReader genericDatumReader =
                    genericDatumReaders.getOrDefault(
                            String.format("%s/%s", eventType, schemaVersion),
                            createGenericDatumReader(eventTypeCache.getEventType(new String(eventType))));
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

    private GenericDatumReader createGenericDatumReader(final EventType eventType) throws IOException {
        return new GenericDatumReader(new Schema.Parser().parse(
                eventType.getSchema().getSchema()));
    }

}
