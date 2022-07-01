package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;

public class TestSchemaProviderService implements SchemaProviderService {

    private final LocalSchemaRegistry localSchemaRegistry;

    public TestSchemaProviderService(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
    }

    @Override
    public Schema getAvroSchema(final String etName, final String version) {
        return localSchemaRegistry.getAvroSchema(etName, version);
    }

    @Override
    public String getAvroSchemaVersion(final String etName, final Schema schema) {
        return null;
    }

    @Override
    public String getSchemaVersion(final String name, final String schema,
                                   final EventTypeSchemaBase.Type type) {
        return null;
    }
}
