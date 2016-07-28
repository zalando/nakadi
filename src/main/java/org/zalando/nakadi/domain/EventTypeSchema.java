package org.zalando.nakadi.domain;

import javax.validation.constraints.NotNull;

public class EventTypeSchema {

    public enum Type {
        JSON_SCHEMA
    }

    @NotNull
    private Type type;

    @NotNull
    private String schema;

    public EventTypeSchema() {}

    public EventTypeSchema(Type type, String schema) {
        this.type = type;
        this.schema = schema;
    }

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(final String schema) {
        this.schema = schema;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final EventTypeSchema that = (EventTypeSchema) o;

        if (type != that.type) return false;
        return schema.equals(that.schema);
    }

    @Override
    public int hashCode() {
        int result = type != null ? type.hashCode() : 0;
        result = 31 * result + (schema != null ? schema.hashCode() : 0);
        return result;
    }
}
