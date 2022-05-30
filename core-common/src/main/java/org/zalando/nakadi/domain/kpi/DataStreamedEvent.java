package org.zalando.nakadi.domain.kpi;

import org.apache.avro.Schema;
import org.zalando.nakadi.config.KPIEventTypes;

public class DataStreamedEvent extends KPIEvent {

    private static final String PATH_SCHEMA = "event-type-schema/nakadi.data.streamed/nakadi.data.streamed.1.avsc";
    private static final Schema SCHEMA;

    static {
        // load latest local schema
        SCHEMA = loadSchema(PATH_SCHEMA);
    }

    @KPIField("event_type")
    private String eventTypeName;
    @KPIField("app")
    private String applicationName;
    @KPIField("app_hashed")
    private String hashedApplicationName;
    @KPIField("token_realm")
    private String tokenRealm;
    @KPIField("number_of_events")
    private long numberOfEvents;
    @KPIField("bytes_streamed")
    private long bytesStreamed;
    @KPIField("batches_streamed")
    private int batchesStreamed;
    @KPIField("api")
    private String api;
    @KPIField("subscription")
    private String subscriptionId;

    public DataStreamedEvent() {
        super(KPIEventTypes.DATA_STREAMED);
    }

    public String getEventTypeName() {
        return eventTypeName;
    }

    public DataStreamedEvent setEventTypeName(final String eventTypeName) {
        this.eventTypeName = eventTypeName;
        return this;
    }

    public String getApplicationName() {
        return applicationName;
    }

    public DataStreamedEvent setApplicationName(final String applicationName) {
        this.applicationName = applicationName;
        return this;
    }

    public String getHashedApplicationName() {
        return hashedApplicationName;
    }

    public DataStreamedEvent setHashedApplicationName(final String hashedApplicationName) {
        this.hashedApplicationName = hashedApplicationName;
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

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public DataStreamedEvent setSubscriptionId(final String subscriptionId) {
        this.subscriptionId = subscriptionId;
        return this;
    }

    @Override
    public Schema getSchema() {
        return SCHEMA;
    }
}
