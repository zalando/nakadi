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
import org.zalando.nakadi.util.AvroUtils;

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
        this.metadataSchema = AvroUtils.getParsedSchema(metadataSchemaRes.getInputStream());
        this.eventTypeSchema = new HashMap<>();

        for (final String eventTypeName : Set.of("nakadi.access.log")) {
            final TreeMap<String, Schema> versionToSchema =
                    loadEventTypeSchemaVersionsFromResource(eventTypeSchemaRes, eventTypeName);
            if (versionToSchema.isEmpty()) {
                throw new NoSuchSchemaException("Not any avro schema found for: " + eventTypeName);
            }
            eventTypeSchema.put(eventTypeName, versionToSchema);
        }
    }

    private TreeMap<String, Schema> loadEventTypeSchemaVersionsFromResource(
            final Resource eventTypeSchemaRes, final String eventTypeName)
        throws IOException {

        final TreeMap<String, Schema> versionToSchema = new TreeMap<>(SCHEMA_VERSION_COMPARATOR);

        for (int i = 0; ; ++i) {
            try {
                final String relativeName = String.format("%s/%s.%d.avsc", eventTypeName, eventTypeName, i);
                final InputStream is = eventTypeSchemaRes.createRelative(relativeName).getInputStream();
                versionToSchema.put(String.valueOf(i), AvroUtils.getParsedSchema(is));
            } catch (final IOException e) {
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
