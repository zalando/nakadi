package org.zalando.nakadi.domain;

import org.apache.avro.Schema;

public class VersionedAvroSchema {
    private final Schema schema;
    private final String version;

    public VersionedAvroSchema(final Schema schema, final String version) {
        this.schema = schema;
        this.version = version;
    }

    public Schema getSchema() {
        return schema;
    }

    public String getVersion() {
        return version;
    }

    public byte getVersionAsByte() {
        return Byte.parseByte(version);
    }
}
