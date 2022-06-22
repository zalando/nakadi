package org.zalando.nakadi.model;

import java.util.Objects;

public class SchemaVersionResponse {

    private final String version;
    private final String schema;

    public SchemaVersionResponse(final String version, final String schema) {
        this.version = version;
        this.schema = schema;
    }

    public String getVersion() {
        return version;
    }

    public String getSchema() {
        return schema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final SchemaVersionResponse that = (SchemaVersionResponse) o;
        return Objects.equals(version, that.version) && Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(version, schema);
    }

    @Override
    public String toString() {
        return "SchemaVersionResponse{" +
                "version='" + version + '\'' +
                ", schema='" + schema + '\'' +
                '}';
    }
}
