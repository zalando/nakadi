package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventTypeSchemaBase;

@Service
public class CombinedSchemaProviderService implements SchemaProviderService {
    private final SchemaService schemaService;
    private final LocalSchemaRegistry localSchemaRegistry;

    @Autowired
    public CombinedSchemaProviderService(final SchemaService schemaService,
                                         final LocalSchemaRegistry localSchemaRegistry) {
        this.schemaService = schemaService;
        this.localSchemaRegistry = localSchemaRegistry;
    }

    @Override
    public Schema getAvroSchema(final String name, final String version) {
        return getSchemaProvider(name).getAvroSchema(name, version);
    }

    @Override
    public String getAvroSchemaVersion(final String name, final Schema schema) {
        return getSchemaProvider(name).getAvroSchemaVersion(name, schema);
    }

    @Override
    public String getSchemaVersion(final String name, final String schema, final EventTypeSchemaBase.Type type) {
        return getSchemaProvider(name).getSchemaVersion(name, schema, type);
    }

    private SchemaProviderService getSchemaProvider(final String name) {
        if (this.localSchemaRegistry.isLocalSchema(name)) {
            return this.localSchemaRegistry;
        } else {
            return this.schemaService;
        }
    }
}
