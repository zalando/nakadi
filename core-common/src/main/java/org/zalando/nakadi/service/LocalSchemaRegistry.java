package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.KPIEventTypes;
import org.zalando.nakadi.domain.VersionedAvroSchema;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.util.AvroUtils;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

// temporarily storage for event type avro schemas untill schema repository supports them
@Service
public class LocalSchemaRegistry {

    public static final String METADATA_KEY = "metadata";
    public static final String BATCH_PUBLISHING_KEY = "batch.publishing";

    private static final Comparator<String> SCHEMA_VERSION_COMPARATOR = Comparator.comparingInt(Integer::parseInt);
    private static final Collection<String> INTERNAL_EVENT_TYPE_NAMES = Set.of(
            METADATA_KEY,
            BATCH_PUBLISHING_KEY,
            KPIEventTypes.ACCESS_LOG,
            KPIEventTypes.BATCH_PUBLISHED,
            KPIEventTypes.DATA_STREAMED,
            KPIEventTypes.EVENT_TYPE_LOG,
            KPIEventTypes.SUBSCRIPTION_LOG);

    private final Map<String, TreeMap<String, Schema>> eventTypeSchema;

    @Autowired
    public LocalSchemaRegistry(
            @Value("${nakadi.avro.schema.root:classpath:avro-schema/}") final Resource eventTypeSchemaRes)
            throws IOException {
        this.eventTypeSchema = new HashMap<>();

        for (final String eventTypeName : INTERNAL_EVENT_TYPE_NAMES) {
            final TreeMap<String, Schema> versionToSchema =
                    loadEventTypeSchemaVersionsFromResource(eventTypeSchemaRes, eventTypeName);
            if (versionToSchema.isEmpty()) {
                throw new NoSuchSchemaException("No avro schema found for: " + eventTypeName);
            }
            eventTypeSchema.put(eventTypeName, versionToSchema);
        }
    }

    public LocalSchemaRegistry(
            final Map<String, TreeMap<String, Schema>> eventTypeSchema) {
        this.eventTypeSchema = eventTypeSchema;
    }

    private TreeMap<String, Schema> loadEventTypeSchemaVersionsFromResource(
            final Resource eventTypeSchemaRes, final String eventTypeName) {

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

    public VersionedAvroSchema getLatestEventTypeSchemaVersion(final String eventTypeName) {
        final var entry = getEventTypeSchemaVersions(eventTypeName).lastEntry();
        return new VersionedAvroSchema(entry.getValue(), entry.getKey());
    }

    public Schema getEventTypeSchema(final String eventTypeName, final String schemaVersion) {
        final Schema schema = getEventTypeSchemaVersions(eventTypeName).get(schemaVersion);
        if (schema == null) {
            throw new NoSuchSchemaException(
                    "Avro schema not found for: " + eventTypeName + ", version " + schemaVersion);
        }
        return schema;
    }

    public TreeMap<String, Schema> getEventTypeSchemaVersions(final String eventTypeName) {
        final TreeMap<String, Schema> versionToSchema = eventTypeSchema.get(eventTypeName);
        if (versionToSchema == null) {
            throw new NoSuchEventTypeException("Avro event type not found: " + eventTypeName);
        }
        return versionToSchema;
    }
}
