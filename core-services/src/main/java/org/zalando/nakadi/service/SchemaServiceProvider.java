package org.zalando.nakadi.service;

import org.apache.avro.Schema;

public interface SchemaServiceProvider {

    Schema getAvroSchema(String etName, String version);
}
