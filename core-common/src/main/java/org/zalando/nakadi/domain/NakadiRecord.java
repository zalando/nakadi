package org.zalando.nakadi.domain;

public class NakadiRecord {

    private byte[] eventKey;
    private byte[] payload;
    private EventOwnerHeader owner;
    private NakadiMetadata metadata;

    public byte[] getEventKey() {
        return eventKey;
    }

    public NakadiRecord setEventKey(final byte[] eventKey) {
        this.eventKey = eventKey;
        return this;
    }

    public byte[] getPayload() {
        return payload;
    }

    public NakadiRecord setPayload(final byte[] payload) {
        this.payload = payload;
        return this;
    }

    public EventOwnerHeader getOwner() {
        return owner;
    }

    public NakadiRecord setOwner(final EventOwnerHeader owner) {
        this.owner = owner;
        return this;
    }

    public NakadiMetadata getMetadata() {
        return metadata;
    }

    public NakadiRecord setMetadata(final NakadiMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

}
