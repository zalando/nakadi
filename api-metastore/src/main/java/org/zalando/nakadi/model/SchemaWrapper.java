package org.zalando.nakadi.model;

import javax.validation.constraints.NotNull;
import java.util.Objects;

public class SchemaWrapper {
    @NotNull
    private String schema;

    public SchemaWrapper() {
    }

    public SchemaWrapper(final String schema) {
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
        final SchemaWrapper that = (SchemaWrapper) o;
        return Objects.equals(schema, that.schema);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schema);
    }

    @Override
    public String toString() {
        return "SchemaWrapper{" +
                "schema='" + schema + '\'' +
                '}';
    }
}
