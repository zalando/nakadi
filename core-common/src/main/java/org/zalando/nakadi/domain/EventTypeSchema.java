package org.zalando.nakadi.domain;

import org.joda.time.DateTime;

public class EventTypeSchema extends EventTypeSchemaBase {

    private String version;

    private DateTime createdAt;

    public EventTypeSchema() {
        super();
    }

    public EventTypeSchema(final EventTypeSchemaBase schemaBase, final String version, final DateTime createdAt) {
        super(schemaBase);
        this.version = version;
        this.createdAt = createdAt;
    }

    public String getVersion() {
        return version;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setVersion(final String version) {
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
