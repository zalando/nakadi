package de.zalando.aruha.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonSetter;
import org.json.JSONObject;

public class EventTypeSchema {

    public static enum Type {
        JSON_SCHEMA
    }

    private Type type;
    private JSONObject schema;

    @JsonGetter("type")
    public Type getType() {
        return type;
    }

    @JsonSetter("type")
    public void setType(final Type type) {
        this.type = type;
    }

    @JsonGetter("schema")
    public JSONObject getSchema() {
        return schema;
    }

    @JsonSetter("schema")
    public void setSchema(final JSONObject schema) {
        this.schema = schema;
    }

}
