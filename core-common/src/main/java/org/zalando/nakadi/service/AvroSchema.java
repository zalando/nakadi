package org.zalando.nakadi.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;

import java.io.InputStream;
import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Set;
import java.util.TreeMap;

// temporarily storage for event type avro schemas untill schema repository supports them
@Service
public class AvroSchema {

    public static final byte METADATA_VERSION = 0;
    public static final Comparator<String> SCHEMA_VERSION_COMPARATOR = Comparator.comparingInt(Integer::parseInt);

    private final Schema metadataSchema;
    private final Map<String, TreeMap<String, Schema>> eventTypeSchema;
    private final AvroMapper avroMapper;
    private final ObjectMapper objectMapper;

    @Autowired
    public AvroSchema(
            final AvroMapper avroMapper,
            final ObjectMapper objectMapper,
            @Value("${nakadi.avro.schema.metadata:classpath:event-type-schema/metadata.avsc}")
            final Resource metadataSchemaRes,
            @Value("${nakadi.avro.schema.internal-event-types:classpath:event-type-schema/}")
            final Resource eventTypeSchemaRes)
            throws IOException {
        this.avroMapper = avroMapper;
        this.objectMapper = objectMapper;
        this.metadataSchema = new Schema.Parser().parse(metadataSchemaRes.getInputStream());
        this.eventTypeSchema = new HashMap<>();

        for (final String eventTypeName : Set.of("nakadi.access.log")) {
            eventTypeSchema.put(eventTypeName,
                    loadEventTypeSchemaVersionsFromResource(eventTypeSchemaRes, eventTypeName));
        }
    }

    private TreeMap<String, Schema> loadEventTypeSchemaVersionsFromResource(
            final Resource eventTypeSchemaRes, final String eventTypeName)
        throws IOException {

        final TreeMap<String, Schema> versionToSchema = new TreeMap<>(SCHEMA_VERSION_COMPARATOR);

        final Resource eventTypeRes = eventTypeSchemaRes.createRelative(eventTypeName);
        for (int i = 0; ; ++i) {
            try {
                final InputStream is = eventTypeRes.createRelative(String.format("%s.%d.avsc", eventTypeName, i))
                    .getInputStream();
                versionToSchema.put(String.valueOf(i), new Schema.Parser().parse(is));
            } catch (IOException e) {
                break;
            }
        }
        return versionToSchema;
    }

    public AvroMapper getAvroMapper() {
        return avroMapper;
    }

    public ObjectMapper getObjectMapper() {
        return objectMapper;
    }

    public Schema getMetadataSchema() {
        return metadataSchema;
    }

    public Map.Entry<String, Schema> getLatestEventTypeSchemaVersion(final String eventTypeName) {
        return getEventTypeSchemaVersions(eventTypeName).lastEntry();
    }

    public Schema getEventTypeSchema(final String eventTypeName, final String schemaVersion) {
        final Schema schema = getEventTypeSchemaVersions(eventTypeName).get(schemaVersion);
        if (schema == null) {
            throw new NoSuchSchemaException(
                    "Avro schema not found for: " + eventTypeName + ", version " + schemaVersion);
        }
        return schema;
    }

    private TreeMap<String, Schema> getEventTypeSchemaVersions(final String eventTypeName) {
        final TreeMap<String, Schema> versionToSchema = eventTypeSchema.get(eventTypeName);
        if (versionToSchema == null) {
            throw new NoSuchEventTypeException("Avro event type not found: " + eventTypeName);
        }
        return versionToSchema;
    }
}
