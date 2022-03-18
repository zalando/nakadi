package org.zalando.nakadi.model;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class CompatibilitySchemaRequest {
    @NotNull
    private String schema;

    public CompatibilitySchemaRequest() {
    }

    public CompatibilitySchemaRequest(final String schema) {
        this.schema = schema;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(final String schema) {
        this.schema = schema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()){
            return false;
        }
        final CompatibilitySchemaRequest that = (CompatibilitySchemaRequest) o;
        return Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public String toString() {
        return "CompatibilitySchemaRequest{" +
                "schema='" + schema + '\'' +
                '}';
    }
}
