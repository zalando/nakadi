package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.time.Instant;

@Component
public class EventMetadata {

    private final UUIDGenerator uuidGenerator;

    public EventMetadata(final UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public JSONObject addTo(final JSONObject event) {
        return event.put("metadata", new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID())
                .put("flow_id", MDCUtils.getFlowId()));
    }
}
