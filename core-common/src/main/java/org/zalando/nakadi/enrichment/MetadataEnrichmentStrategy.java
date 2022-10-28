package org.zalando.nakadi.enrichment;

import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONException;
import org.json.JSONObject;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeSchema;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.EnrichmentException;
import org.zalando.nakadi.exceptions.runtime.NoSuchSchemaException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.util.MDCUtils;

import java.time.Instant;
import java.util.Optional;

public class MetadataEnrichmentStrategy implements EnrichmentStrategy {

    private final AuthorizationService authorizationService;

    public MetadataEnrichmentStrategy(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    @Override
    public void enrich(final BatchItem batchItem, final EventType eventType)
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
            setVersion(metadata, eventType);
            batchItem.inject(BatchItem.Injection.METADATA, metadata.toString());
        } catch (final JSONException e) {
            throw new EnrichmentException("enrichment error", e);
        }
    }

    @Override
    public void enrich(final NakadiRecord nakadiRecord, final EventType eventType) throws EnrichmentException {
        final var metadata = nakadiRecord.getMetadata();
        metadata.setPublishedBy(getPublisher());
        metadata.setReceivedAt(Instant.now());
        if (metadata.getFlowId() == null || metadata.getFlowId().isEmpty()) {
            metadata.setFlowId(MDCUtils.getFlowId());
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

    private void setVersion(final JSONObject metadata, final EventType eventType) {
        final Optional<EventTypeSchema> schema = eventType.getLatestSchemaByType(EventTypeSchema.Type.JSON_SCHEMA);
        if (!schema.isPresent()) {
            throw new NoSuchSchemaException("No json_schema found for event type: " + eventType.getName());
        }
        metadata.put("version", schema.get().getVersion());
    }

    private void setFlowId(final JSONObject metadata) {
        if ("".equals(metadata.optString("flow_id"))) {
            metadata.put("flow_id", MDCUtils.getFlowId());
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
