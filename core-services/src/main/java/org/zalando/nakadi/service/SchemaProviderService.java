package org.zalando.nakadi.service;

import org.apache.avro.Schema;

public interface SchemaProviderService {

    Schema getAvroSchema(String etName, String version);

    String getAvroSchemaVersion(String etName, Schema schema);
}
