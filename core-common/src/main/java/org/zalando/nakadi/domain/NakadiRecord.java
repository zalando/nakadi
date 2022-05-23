package org.zalando.nakadi.domain;

import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;

public class NakadiRecord {

    public static final String HEADER_FORMAT = new String(new byte[]{0});

    public enum Format {
        AVRO(new byte[]{0});

        private final byte[] format;

        Format(final byte[] format) {
            this.format = format;
        }

        public byte[] getFormat() {
            return this.format;
        }
    }

    private byte[] eventKey;
    private byte[] payload;
    private byte[] format;
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

    public byte[] getFormat() {
        return format;
    }

    public NakadiRecord setFormat(final byte[] format) {
        this.format = format;
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

    public ProducerRecord<byte[], byte[]> toProducerRecord(final String topic) throws IOException {

        final var partition = metadata.getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;

        final var eventData = EnvelopeHolder.produceBytes(
                metadata.getMetadataVersion(), metadata, os -> os.write(payload));

        return new ProducerRecord<>(topic, partitionInt, eventKey, eventData);
    }
}
