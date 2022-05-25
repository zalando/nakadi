package org.zalando.nakadi.service;

import org.apache.avro.Schema;

public class TestSchemaProviderService implements SchemaProviderService {

    private final LocalSchemaRegistry localSchemaRegistry;

    public TestSchemaProviderService(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
    }

    @Override
    public Schema getAvroSchema(final String etName, final String version) {
        return localSchemaRegistry.getEventTypeSchema(etName, version);
    }
}
