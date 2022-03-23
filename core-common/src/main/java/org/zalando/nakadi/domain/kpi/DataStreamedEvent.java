package org.zalando.nakadi.domain.kpi;

import org.json.JSONObject;

public class DataStreamedEvent {
    private String eventType;
    private String app;
    private String appHashed;
    private String tokenRealm;
    private long numberOfEvents;
    private long bytesStreamed;
    private int batchesStreamed;
    private String api;
    private String subscription;

    public String getEventType() {
        return eventType;
    }

    public DataStreamedEvent setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public String getApp() {
        return app;
    }

    public DataStreamedEvent setApp(final String app) {
        this.app = app;
        return this;
    }

    public String getAppHashed() {
        return appHashed;
    }

    public DataStreamedEvent setAppHashed(final String appHashed) {
        this.appHashed = appHashed;
        return this;
    }

    public String getTokenRealm() {
        return tokenRealm;
    }

    public DataStreamedEvent setTokenRealm(final String tokenRealm) {
        this.tokenRealm = tokenRealm;
        return this;
    }

    public long getNumberOfEvents() {
        return numberOfEvents;
    }

    public DataStreamedEvent setNumberOfEvents(final long numberOfEvents) {
        this.numberOfEvents = numberOfEvents;
        return this;
    }

    public long getBytesStreamed() {
        return bytesStreamed;
    }

    public DataStreamedEvent setBytesStreamed(final long bytesStreamed) {
        this.bytesStreamed = bytesStreamed;
        return this;
    }

    public int getBatchesStreamed() {
        return batchesStreamed;
    }

    public DataStreamedEvent setBatchesStreamed(final int batchesStreamed) {
        this.batchesStreamed = batchesStreamed;
        return this;
    }

    public String getApi() {
        return api;
    }

    public DataStreamedEvent setApi(final String api) {
        this.api = api;
        return this;
    }

    public String getSubscription() {
        return subscription;
    }

    public DataStreamedEvent setSubscription(final String subscription) {
        this.subscription = subscription;
        return this;
    }

    public JSONObject asJsonObject() {
        final JSONObject json = new JSONObject()
                .put("event_type", this.getEventType())
                .put("app", this.getApp())
                .put("app_hashed", this.getAppHashed())
                .put("token_realm", this.getTokenRealm())
                .put("number_of_events", this.getNumberOfEvents())
                .put("bytes_streamed", this.getBytesStreamed())
                .put("batches_streamed", this.getBatchesStreamed())
                .put("api", this.getApi())
                .putOpt("subscription", this.getSubscription());

        return json;
    }

}
