package org.zalando.nakadi.enrichment;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.util.FlowIdUtils;

public class MetadataEnrichmentStrategy implements EnrichmentStrategy {
    @Override
    public void enrich(final BatchItem batchItem, final EventType eventType) throws EnrichmentException {
        try {
            final JSONObject metadata = batchItem
                    .getEvent()
                    .getEventJson()
                    .getJSONObject(BatchItem.Injection.METADATA.name);

            setReceivedAt(metadata);
            setEventTypeName(metadata, eventType);
            setFlowId(metadata);
            setPartition(metadata, batchItem);
            setVersion(metadata, eventType);
            batchItem.inject(BatchItem.Injection.METADATA, metadata.toString());
        } catch (final JSONException e) {
            throw new EnrichmentException("enrichment error", e);
        }
    }

    private void setVersion(final JSONObject metadata, final EventType eventType) {
        metadata.put("version", eventType.getSchema().getVersion().toString());
    }

    private void setFlowId(final JSONObject metadata) {
        if ("".equals(metadata.optString("flow_id"))) {
            metadata.put("flow_id", FlowIdUtils.peek());
        }
    }

    private void setEventTypeName(final JSONObject metadata, final EventType eventType) {
        metadata.put("event_type", eventType.getName());
    }

    private void setReceivedAt(final JSONObject metadata) {
        final DateTime dateTime = new DateTime(DateTimeZone.UTC);
        metadata.put("received_at", dateTime.toString());
    }

    public void setPartition(final JSONObject metadata, final BatchItem batchItem) {
        metadata.put("partition", batchItem.getPartition());
    }
}
