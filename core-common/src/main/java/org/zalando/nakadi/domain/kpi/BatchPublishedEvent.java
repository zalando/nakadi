package org.zalando.nakadi.domain.kpi;

import org.apache.avro.generic.IndexedRecord;

public class BatchPublishedEvent {
    private String eventType;
    private String app;
    private String appHashed;
    private String tokenRealm;
    private int numberOfEvents;
    private long msSpent;
    private int batchSize;

    public String getEventType() {
        return eventType;
    }

    public BatchPublishedEvent setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getApp() {
        return app;
    }

    public BatchPublishedEvent setApp(final String app) {
        this.app = app;
        return this;
    }

    public String getAppHashed() {
        return appHashed;
    }

    public BatchPublishedEvent setAppHashed(final String appHashed) {
        this.appHashed = appHashed;
        return this;
    }

    public String getTokenRealm() {
        return tokenRealm;
    }

    public BatchPublishedEvent setTokenRealm(final String tokenRealm) {
        this.tokenRealm = tokenRealm;
        return this;
    }

    public int getNumberOfEvents() {
        return numberOfEvents;
    }

    public BatchPublishedEvent setNumberOfEvents(final int numberOfEvents) {
        this.numberOfEvents = numberOfEvents;
        return this;
    }

    public long getMsSpent() {
        return msSpent;
    }

    public BatchPublishedEvent setMsSpent(final long msSpent) {
        this.msSpent = msSpent;
        return this;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public BatchPublishedEvent setBatchSize(final int batchSize) {
        this.batchSize = batchSize;
        return this;
    }

    public IndexedRecord mapToAvroRecordV01() {
        return NakadiBatchPublishedEventV01.newBuilder()
                .setEventType(eventType)
                .setApp(app)
                .setAppHashed(appHashed)
                .setTokenRealm(tokenRealm)
                .setNumberOfEvents(numberOfEvents)
                .setMsSpent(msSpent)
                .setBatchSize(batchSize)
                .build();
    }

}
