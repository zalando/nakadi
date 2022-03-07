package org.zalando.nakadi.domain;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.As;
import static com.fasterxml.jackson.annotation.JsonTypeInfo.Id;

import org.joda.time.DateTime;

public class EventTypeSchema extends EventTypeSchemaBase {
    @JsonTypeInfo(use = Id.NAME, property = "type", include = As.EXTERNAL_PROPERTY)
    @JsonSubTypes(value = {
            @JsonSubTypes.Type(value = JsonVersion.class, name = "JSON_SCHEMA"),
            @JsonSubTypes.Type(value = AvroVersion.class, name = "AVRO_SCHEMA")
    })
    private Version version;

    private DateTime createdAt;

    public EventTypeSchema() {
        super();
    }

    public EventTypeSchema(final EventTypeSchemaBase schemaBase, final String version, final DateTime createdAt) {
        super(schemaBase);
        this.version = this.getType().equals(Type.JSON_SCHEMA)? new JsonVersion(version) : new AvroVersion(version);
        this.createdAt = createdAt;
    }

    public Version getVersion() {
        return version;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setVersion(final Version version) {
        this.version = version;
    }

    public void setCreatedAt(final DateTime createdAt) {
        this.createdAt = createdAt;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        final EventTypeSchema that = (EventTypeSchema) o;

        if (!version.equals(that.version)) {
            return false;
        }
        return createdAt.equals(that.createdAt);

    }

    @Override
    public int hashCode() {
        int result = super.hashCode();
        result = 31 * result + version.hashCode();
        result = 31 * result + createdAt.hashCode();
        return result;
    }
}
