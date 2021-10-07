package org.zalando.nakadi.service.publishing;

import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.util.UUIDGenerator;

import java.time.Instant;

@Component
public class MetadataService {

    private final UUIDGenerator uuidGenerator;

    public MetadataService(final UUIDGenerator uuidGenerator) {
        this.uuidGenerator = uuidGenerator;
    }

    public void enrich(final JSONObject event) {
        event.put("metadata", new JSONObject()
                .put("occurred_at", Instant.now())
                .put("eid", uuidGenerator.randomUUID())
                .put("flow_id", FlowIdUtils.peek()));
    }
}
