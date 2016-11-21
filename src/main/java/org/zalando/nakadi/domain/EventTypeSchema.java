package org.zalando.nakadi.domain;

import org.joda.time.DateTime;

public class EventTypeSchema extends EventTypeSchemaBase {
    private Version version;
    private DateTime createdAt;

    public EventTypeSchema() {
        super();
    }

    public EventTypeSchema(final EventTypeSchemaBase schemaBase, final String version, final DateTime createdAt) {
        super(schemaBase);
        this.version = new Version(version);
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

}
