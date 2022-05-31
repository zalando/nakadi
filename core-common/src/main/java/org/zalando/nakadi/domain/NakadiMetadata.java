package org.zalando.nakadi.domain;

import java.util.List;

public abstract class NakadiMetadata implements EnvelopeHolder.EventWriter {

    private final byte metadataVersion;

    private String eid;
    private Long occurredAt;
    private String eventType;
    private String partition;
    private String publishedBy;
    private Long receivedAt;
    private String flowId;
    private String spanCtx;
    private String schemaVersion;
    private List<String> parentEids;
    private List<String> partitionKeys;
    private String partitionCompactionKey;
    private String eventOwner;

    public NakadiMetadata(final byte metadataVersion) {
        this.metadataVersion = metadataVersion;
    }

    public byte getMetadataVersion() {
        return metadataVersion;
    }

    public String getEid() {
        return eid;
    }

    public NakadiMetadata setEid(final String eid) {
        this.eid = eid;
        return this;
    }

    public Long getOccurredAt() {
        return occurredAt;
    }

    public NakadiMetadata setOccurredAt(final Long occurredAt) {
        this.occurredAt = occurredAt;
        return this;
    }

    public String getEventType() {
        return eventType;
    }

    public NakadiMetadata setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getPartition() {
        return partition;
    }

    public NakadiMetadata setPartition(final String partition) {
        this.partition = partition;
        return this;
    }

    public String getPublishedBy() {
        return publishedBy;
    }

    public NakadiMetadata setPublishedBy(final String publishedBy) {
        this.publishedBy = publishedBy;
        return this;
    }

    public Long getReceivedAt() {
        return receivedAt;
    }

    public NakadiMetadata setReceivedAt(final Long receivedAt) {
        this.receivedAt = receivedAt;
        return this;
    }

    public String getFlowId() {
        return flowId;
    }

    public NakadiMetadata setFlowId(final String flowId) {
        this.flowId = flowId;
        return this;
    }

    public String getSpanCtx() {
        return spanCtx;
    }

    public NakadiMetadata setSpanCtx(final String spanCtx) {
        this.spanCtx = spanCtx;
        return this;
    }

    public String getSchemaVersion() {
        return schemaVersion;
    }

    public NakadiMetadata setSchemaVersion(final String schemaVersion) {
        this.schemaVersion = schemaVersion;
        return this;
    }

    public List<String> getParentEids() {
        return parentEids;
    }

    public NakadiMetadata setParentEids(final List<String> parentEids) {
        this.parentEids = parentEids;
        return this;
    }

    public List<String> getPartitionKeys() {
        return partitionKeys;
    }

    public NakadiMetadata setPartitionKeys(final List<String> partitionKeys) {
        this.partitionKeys = partitionKeys;
        return this;
    }

    public String getPartitionCompactionKey() {
        return partitionCompactionKey;
    }

    public NakadiMetadata setPartitionCompactionKey(final String partitionCompactionKey) {
        this.partitionCompactionKey = partitionCompactionKey;
        return this;
    }

    public String getEventOwner() {
        return eventOwner;
    }

    public NakadiMetadata setEventOwner(final String eventOwner) {
        this.eventOwner = eventOwner;
        return this;
    }
}
