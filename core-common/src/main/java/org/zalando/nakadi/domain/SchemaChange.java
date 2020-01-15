package org.zalando.nakadi.domain;

public class SchemaChange {
    public enum Type {
        ID_CHANGED(Version.Level.MAJOR),
        DESCRIPTION_CHANGED(Version.Level.PATCH),
        TITLE_CHANGED(Version.Level.PATCH),
        PROPERTIES_ADDED(Version.Level.MINOR),
        SCHEMA_REMOVED(Version.Level.MAJOR),
        TYPE_CHANGED(Version.Level.MAJOR),
        NUMBER_OF_ITEMS_CHANGED(Version.Level.MAJOR),
        PROPERTY_REMOVED(Version.Level.MAJOR),
        DEPENDENCY_ARRAY_CHANGED(Version.Level.MAJOR),
        DEPENDENCY_SCHEMA_CHANGED(Version.Level.MAJOR),
        COMPOSITION_METHOD_CHANGED(Version.Level.MAJOR),
        ATTRIBUTE_VALUE_CHANGED(Version.Level.MAJOR),
        ENUM_ARRAY_CHANGED(Version.Level.MAJOR),
        SUB_SCHEMA_CHANGED(Version.Level.MAJOR),
        DEPENDENCY_SCHEMA_REMOVED(Version.Level.MAJOR),
        REQUIRED_ARRAY_CHANGED(Version.Level.MAJOR),
        REQUIRED_ARRAY_EXTENDED(Version.Level.MINOR, Version.Level.MAJOR),
        ADDITIONAL_PROPERTIES_CHANGED(Version.Level.MAJOR),
        ADDITIONAL_PROPERTIES_NARROWED(Version.Level.MINOR, Version.Level.MAJOR),
        ADDITIONAL_ITEMS_CHANGED(Version.Level.MAJOR),
        ;
        public final Version.Level levelForForward;
        public final Version.Level levelForCompatible;

        Type(final Version.Level levelForAll) {
            this(levelForAll, levelForAll);
        }

        Type(final Version.Level levelForForward, final Version.Level levelForCompatible) {
            this.levelForForward = levelForForward;
            this.levelForCompatible = levelForCompatible;
        }

        public Version.Level getLevel(final CompatibilityMode mode) {
            return CompatibilityMode.COMPATIBLE == mode ? levelForCompatible : levelForForward;
        }
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
