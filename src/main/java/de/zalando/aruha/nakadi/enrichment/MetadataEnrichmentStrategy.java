package de.zalando.aruha.nakadi.enrichment;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.EnrichmentException;
import de.zalando.aruha.nakadi.util.FlowIdUtils;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.json.JSONObject;

public class MetadataEnrichmentStrategy implements EnrichmentStrategy {
    @Override
    public void enrich(final JSONObject event, final EventType eventType) throws EnrichmentException {
        try {
            final JSONObject metadata = event.getJSONObject("metadata");

            setReceivedAt(metadata);
            setEventTypeName(metadata, eventType);
            setFlowId(metadata);
        } catch (final JSONException e) {
            throw new EnrichmentException("enrichment error", e);
        }
    }

    private void setFlowId(final JSONObject metadata) {
        metadata.put("flow_id", FlowIdUtils.peek());
    }

    private void setEventTypeName(final JSONObject metadata, final EventType eventType) {
        metadata.put("event_type", eventType.getName());
    }

    private void setReceivedAt(final JSONObject metadata) {
        final DateTime dateTime = new DateTime(DateTimeZone.UTC);
        metadata.put("received_at", dateTime.toString());
    }
}
