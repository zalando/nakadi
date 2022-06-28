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
        return localSchemaRegistry.getEventTypeSchema(etName, version);
    }

    @Override
    public String getAvroSchemaVersion(final String etName, final Schema schema) {
        return null;
    }

    @Override
    public String getSchemaVersion(String name, String schema, EventTypeSchemaBase.Type type) {
        return null;
    }
}
