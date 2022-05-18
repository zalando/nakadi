package org.zalando.nakadi.service;

import org.apache.avro.Schema;

public class TestSchemaServiceProvider implements SchemaServiceProvider {

    private final AvroSchema avroSchema;

    public TestSchemaServiceProvider(final AvroSchema avroSchema) {
        this.avroSchema = avroSchema;
    }

    @Override
    public Schema getAvroSchema(final String etName, final String version) {
        return avroSchema.getEventTypeSchema(etName, version);
    }
}
