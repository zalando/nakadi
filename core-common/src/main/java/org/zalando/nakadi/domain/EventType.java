package org.zalando.nakadi.domain;

import org.joda.time.DateTime;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public class EventType extends EventTypeBase {

    private DateTime updatedAt;
    private DateTime createdAt;

    private EventTypeSchema schema;
    private final Map<EventTypeSchema.Type, EventTypeSchema> latestSchemas = new HashMap<>();

    public EventType(final EventTypeBase eventType, final String version, final DateTime createdAt,
                     final DateTime updatedAt) {
        super(eventType);
        this.updatedAt = updatedAt;
        this.createdAt = createdAt;
        this.setSchema(new EventTypeSchema(eventType.getSchema(), version, updatedAt));
    }

    public EventType(final EventTypeBase eventType, final DateTime createdAt, final DateTime updatedAt,
                     final EventTypeSchema eventTypeSchema) {
        super(eventType);
        this.updatedAt = updatedAt;
        this.createdAt = createdAt;
        this.setSchema(eventTypeSchema);
    }

    public EventType() {
        super();
    }

    public EventTypeSchema getSchema() {
        return schema;
    }

    public void setSchema(final EventTypeSchema schema) {
        if (schema == null) {
            throw new IllegalArgumentException("The event type schema must not be null!");
        }
        this.schema = schema;
        setLatestSchemaByType(schema);
    }

    public Optional<EventTypeSchema> getLatestSchemaByType(final EventTypeSchema.Type schemaType) {
        return Optional.ofNullable(latestSchemas.get(schemaType));
    }

    public void setLatestSchemaByType(final EventTypeSchema schema) {
        latestSchemas.put(schema.getType(), schema);
    }

    public DateTime getUpdatedAt() {
        return updatedAt;
    }

    public DateTime getCreatedAt() {
        return createdAt;
    }

    public void setUpdatedAt(final DateTime updatedAt) {
        this.updatedAt = updatedAt;
    }

    public void setCreatedAt(final DateTime createdAt) {
        this.createdAt = createdAt;
    }

    public Resource<EventType> asResource() {
        return new ResourceImpl<>(getName(), ResourceImpl.EVENT_TYPE_RESOURCE, getAuthorization(), this);
    }
}
