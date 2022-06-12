package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;
import org.zalando.nakadi.config.KPIEventTypes;

public class BatchPublishedEvent extends KPIEvent {

    private static final String PATH_SCHEMA = "event-type-schema/nakadi.batch.published/nakadi.batch.published.0.avsc";
    private static final Schema SCHEMA = loadSchema(PATH_SCHEMA);

    @KPIField("event_type")
    private String eventTypeName;
    @KPIField("app")
    private String applicationName;
    @KPIField("app_hashed")
    private String hashedApplicationName;
    @KPIField("token_realm")
    private String tokenRealm;
    @KPIField("number_of_events")
    private int eventCount;
    @KPIField("ms_spent")
    private long msSpent;
    @KPIField("batch_size")
    private int totalSizeBytes;

    public BatchPublishedEvent() {
        super(KPIEventTypes.BATCH_PUBLISHED);
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public BatchPublishedEvent setEventTypeName(final String eventTypeName) {
        this.eventTypeName = eventTypeName;
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public BatchPublishedEvent setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
        return this;
    }

    public String getHashedApplicationName() {
        return hashedApplicationName;
    }

    public BatchPublishedEvent setHashedApplicationName(final String hashedApplicationName) {
        this.hashedApplicationName = hashedApplicationName;
        return this;
    }

    public String getTokenRealm() {
        return tokenRealm;
    }

    public BatchPublishedEvent setTokenRealm(final String tokenRealm) {
        this.tokenRealm = tokenRealm;
        return this;
    }

    public int getEventCount() {
        return eventCount;
    }

    public BatchPublishedEvent setEventCount(final int eventCount) {
        this.eventCount = eventCount;
        return this;
    }

    public long getMsSpent() {
        return msSpent;
    }

    public BatchPublishedEvent setMsSpent(final long msSpent) {
        this.msSpent = msSpent;
        return this;
    }

    public int getTotalSizeBytes() {
        return totalSizeBytes;
    }

    public BatchPublishedEvent setTotalSizeBytes(final int totalSizeBytes) {
        this.totalSizeBytes = totalSizeBytes;
        return this;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
