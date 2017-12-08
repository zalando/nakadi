package org.zalando.nakadi.domain;

public class SchemaChange {
    public enum Type {
        ID_CHANGED,
        DESCRIPTION_CHANGED,
        TITLE_CHANGED,
        PROPERTIES_ADDED,
        SCHEMA_REMOVED,
        TYPE_CHANGED,
        NUMBER_OF_ITEMS_CHANGED,
        PROPERTY_REMOVED,
        DEPENDENCY_ARRAY_CHANGED,
        DEPENDENCY_SCHEMA_CHANGED,
        COMPOSITION_METHOD_CHANGED,
        ATTRIBUTE_VALUE_CHANGED,
        ENUM_ARRAY_CHANGED,
        SUB_SCHEMA_CHANGED,
        DEPENDENCY_SCHEMA_REMOVED,
        REQUIRED_ARRAY_CHANGED,
        REQUIRED_ARRAY_EXTENDED,
        ADDITIONAL_PROPERTIES_CHANGED,
        ADDITIONAL_ITEMS_CHANGED
    }

    private final String jsonPath;
    private final Type type;

    public String getJsonPath() {
        return jsonPath;
    }

    public Type getType() {
        return type;
    }

    public SchemaChange(final SchemaChange.Type type, final String jsonPath) {
        this.jsonPath = jsonPath;
        this.type = type;
    }
}
