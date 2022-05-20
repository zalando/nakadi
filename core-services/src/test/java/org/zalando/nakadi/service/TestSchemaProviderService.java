package org.zalando.nakadi.service;

import org.apache.avro.Schema;

public class TestSchemaProviderService implements SchemaProviderService {

    private final AvroSchema avroSchema;

    public TestSchemaProviderService(final AvroSchema avroSchema) {
        this.avroSchema = avroSchema;
    }

    @Override
    public Schema getAvroSchema(final String etName, final String version) {
        return avroSchema.getEventTypeSchema(etName, version);
    }
}
