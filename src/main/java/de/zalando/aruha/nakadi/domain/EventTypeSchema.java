package de.zalando.aruha.nakadi.domain;

import javax.validation.constraints.NotNull;

public class EventTypeSchema {

    public static enum Type {
        JSON_SCHEMA
    }

    @NotNull
    private Type type;

    @NotNull
    private String schema;

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

}
