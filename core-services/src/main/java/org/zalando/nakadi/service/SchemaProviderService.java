package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.zalando.nakadi.domain.EventTypeSchemaBase;

public interface SchemaProviderService {

    Schema getAvroSchema(String etName, String version);

    String getAvroSchemaVersion(String etName, Schema schema);

    String getSchemaVersion(String name, String schema, EventTypeSchemaBase.Type type);
}
