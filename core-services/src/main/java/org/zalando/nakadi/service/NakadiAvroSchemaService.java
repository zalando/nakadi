package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class NakadiAvroSchemaService {

    public static final class VersionedAvroSchema {
        private final Schema avroSchema;
        private final String version;

        public VersionedAvroSchema(final Schema avroSchema, final String version) {
            this.avroSchema = avroSchema;
            this.version = version;
        }

        public Schema getAvroSchema() {
            return avroSchema;
        }

        public String getVersion() {
            return version;
        }
    }

    private final LocalSchemaRegistry localSchemaRegistry;

    @Autowired
    public NakadiAvroSchemaService(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
    }

    public List<VersionedAvroSchema> getNakadiAvroSchemas(final String schemaName) {
        final var schemas = localSchemaRegistry.getEventTypeSchemaVersions(schemaName);
        return schemas.entrySet().stream()
                .map(entry -> new VersionedAvroSchema(entry.getValue(), entry.getKey()))
                .collect(Collectors.toList());
    }

    public Schema getNakadiAvroSchemas(final String schemaName, final String version) {
        return localSchemaRegistry.getEventTypeSchema(schemaName, version);
    }
}
