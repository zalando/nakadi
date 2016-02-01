package de.zalando.aruha.nakadi.domain;

import org.json.JSONObject;

public class EventTypeSchema {

    public static enum Type {
        JSON_SCHEMA
    }

    private Type type;
    private JSONObject schema;

    public Type getType() {
        return type;
    }

    public void setType(final Type type) {
        this.type = type;
    }

    public JSONObject getSchema() {
        return schema;
    }

    public void setSchema(final JSONObject schema) {
        this.schema = schema;
    }

}
