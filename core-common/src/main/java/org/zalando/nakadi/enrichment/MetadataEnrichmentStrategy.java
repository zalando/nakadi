package org.zalando.nakadi.enrichment;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.util.FlowIdUtils;

public class MetadataEnrichmentStrategy implements EnrichmentStrategy {

    private final AuthorizationService authorizationService;

    public MetadataEnrichmentStrategy(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    @Override
    public void enrich(final BatchItem batchItem, final EventType eventType, final String schemaVersion)
            throws EnrichmentException {
        try {
            final JSONObject metadata = batchItem
                    .getEvent()
                    .getJSONObject(BatchItem.Injection.METADATA.name);

            setPublisher(metadata);
            setReceivedAt(metadata);
            setEventTypeName(metadata, eventType);
            setFlowId(metadata);
            setPartition(metadata, batchItem);
            setVersion(metadata, schemaVersion);
            batchItem.inject(BatchItem.Injection.METADATA, metadata.toString());
        } catch (final JSONException e) {
            throw new EnrichmentException("enrichment error", e);
        }
    }

    @Override
    public void enrich(final NakadiRecord nakadiRecord, final EventType eventType) throws EnrichmentException {
        final var metadata = nakadiRecord.getMetadata();
        metadata.setPublishedBy(getPublisher());
        final DateTime dateTime = new DateTime(DateTimeZone.UTC);
        metadata.setReceivedAt(dateTime.getMillis());
        if (metadata.getFlowId() == null || metadata.getFlowId().isEmpty()) {
            metadata.setFlowId(FlowIdUtils.peek());
        }
    }

    private void setPublisher(final JSONObject metadata) {
        final String publisher = getPublisher();
        metadata.put("published_by", publisher);
    }

    private String getPublisher() {
        return authorizationService.getSubject().map(Subject::getName)
                .orElse(SecuritySettings.UNAUTHENTICATED_CLIENT_ID);
    }

    private void setVersion(final JSONObject metadata, final String version) {
        metadata.put("version", version);
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
