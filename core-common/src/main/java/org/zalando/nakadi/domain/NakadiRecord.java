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
    private EnvelopeHolder envelope;
    private byte[] format;
    private EventOwnerHeader owner;
    private NakadiAvroMetadata metadata;
    private String partition;

    public byte[] getEventKey() {
        return eventKey;
    }

    public NakadiRecord setEventKey(final byte[] eventKey) {
        this.eventKey = eventKey;
        return this;
    }

    public EnvelopeHolder getEnvelope() {
        return envelope;
    }

    public NakadiRecord setEnvelope(final EnvelopeHolder envelope) {
        this.envelope = envelope;
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

    public NakadiAvroMetadata getMetadata() {
        return metadata;
    }

    public NakadiRecord setMetadata(final NakadiAvroMetadata metadata) {
        this.metadata = metadata;
        return this;
    }

    public String getPartition() {
        return partition;
    }

    public NakadiRecord setPartition(final String partition) {
        this.partition = partition;
        return this;
    }

    public ProducerRecord<byte[], byte[]> toProducerRecord(final String topic) throws IOException {

        final var partition = metadata.getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;

        final var eventData = EnvelopeHolder.produceBytes(
                metadata.getMetadataVersion(), metadata, envelope.getPayloadWriter());

        return new ProducerRecord<>(topic, partitionInt, eventKey, eventData);
    }
}
