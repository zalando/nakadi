package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;
import org.zalando.nakadi.config.KPIEventTypes;
import org.zalando.nakadi.util.AvroUtils;

import java.io.IOException;

public class EventTypeLogEvent extends KPIEvent {

    private static final String PATH_SCHEMA = "event-type-schema/nakadi.event.type.log/nakadi.event.type.log.1.avsc";
    private static final Schema SCHEMA;

    static {
        // load latest local schema
        try {
            SCHEMA = AvroUtils.getParsedSchema(
                    KPIEvent.class.getClassLoader().getResourceAsStream(PATH_SCHEMA)
            );
        } catch (IOException e) {
            throw new RuntimeException("failed to load avro schema");
        }
    }

    @KPIField("event_type")
    private String eventType;
    @KPIField("status")
    private String status;
    @KPIField("category")
    private String category;
    @KPIField("authz")
    private String authz;
    @KPIField("compatibility_mode")
    private String compatibilityMode;

    public EventTypeLogEvent() {
        super(KPIEventTypes.EVENT_TYPE_LOG);
    }

    public String getEventType() {
        return eventType;
    }

    public EventTypeLogEvent setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public EventTypeLogEvent setStatus(final String status) {
        this.status = status;
        return this;
    }

    public String getCategory() {
        return category;
    }

    public EventTypeLogEvent setCategory(final String category) {
        this.category = category;
        return this;
    }

    public String getAuthz() {
        return authz;
    }

    public EventTypeLogEvent setAuthz(final String authz) {
        this.authz = authz;
        return this;
    }

    public String getCompatibilityMode() {
        return compatibilityMode;
    }

    public EventTypeLogEvent setCompatibilityMode(final String compatibilityMode) {
        this.compatibilityMode = compatibilityMode;
        return this;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
