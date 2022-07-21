package org.zalando.nakadi.domain;

import java.nio.charset.StandardCharsets;

public class NakadiRecord {

    private String eventKey;
    private byte[] payload;
    private EventOwnerHeader owner;
    private NakadiMetadata metadata;

    public String getEventKey() {
        return eventKey;
    }

    public byte[] getEventKeyBytes() {
        if (eventKey == null) {
            return null;
        }

        return eventKey.getBytes(StandardCharsets.UTF_8);
    }

    public NakadiRecord setEventKey(final String eventKey) {
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
