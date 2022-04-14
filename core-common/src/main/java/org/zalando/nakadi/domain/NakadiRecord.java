package org.zalando.nakadi.domain;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NakadiRecord implements Resource<NakadiRecord> {

    public static final String HEADER_FORMAT = new String(new byte[]{0});

    // todo maybe use a dedicated resource object
    @Override
    public String getName() {
        return metadata.get("eid").toString();
    }

    @Override
    public String getType() {
        return ResourceImpl.EVENT_RESOURCE;
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        if (operation == AuthorizationService.Operation.WRITE) {
            return Optional.ofNullable(owner).map(AuthorizationAttributeProxy::new).map(Collections::singletonList);
        }
        return Optional.empty();
    }

    @Override
    public NakadiRecord get() {
        return this;
    }

    @Override
    public Map<String, List<AuthorizationAttribute>> getAuthorization() {
        return null;
    }

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

    private String eventType;
    private Integer partition;
    private byte[] eventKey;
    private byte[] data;
    private byte[] format;
    private EventOwnerHeader owner;
    private byte metadataVersion;
    // fixme should be some generic type to support other formats eg proto
    private GenericRecord metadata;

    public String getEventType() {
        return eventType;
    }

    public Integer getPartition() {
        return partition;
    }

    public byte[] getEventKey() {
        return eventKey;
    }

    public byte[] getData() {
        return data;
    }

    public byte[] getFormat() {
        return format;
    }

    public EventOwnerHeader getOwner() {
        return owner;
    }

    public GenericRecord getEventMetadata() {
        return metadata;
    }

    public byte getMetadataVersion() {
        return metadataVersion;
    }

    public NakadiRecord setEventType(final String eventType) {
        this.eventType = eventType;
        return this;
    }

    public NakadiRecord setPartition(final Integer partition) {
        this.partition = partition;
        return this;
    }

    public NakadiRecord setEventKey(final byte[] eventKey) {
        this.eventKey = eventKey;
        return this;
    }

    public NakadiRecord setData(final byte[] data) {
        this.data = data;
        return this;
    }

    public NakadiRecord setFormat(final byte[] format) {
        this.format = format;
        return this;
    }

    public NakadiRecord setOwner(final EventOwnerHeader owner) {
        this.owner = owner;
        return this;
    }

    public NakadiRecord setEventMetadata(final GenericRecord metadata) {
        this.metadata = metadata;
        return this;
    }

    public NakadiRecord setEventMetadataVersion(final byte version) {
        this.metadataVersion = version;
        return this;
    }

    public static NakadiRecord fromAvro(final String eventTypeName,
                                        final byte metadataVersion,
                                        final GenericRecord metadata,
                                        final GenericRecord event) throws IOException {
        final byte[] data = EnvelopeHolder.produceBytes(
                metadataVersion,
                (outputStream -> {
                    final GenericDatumWriter eventWriter = new GenericDatumWriter(metadata.getSchema());
                    eventWriter.write(metadata, EncoderFactory.get()
                            .directBinaryEncoder(outputStream, null));
                }),
                (outputStream -> {
                    final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
                    eventWriter.write(event, EncoderFactory.get()
                            .directBinaryEncoder(outputStream, null));
                }));
        return new NakadiRecord()
                .setEventType(eventTypeName)
                .setPartition(null)
                .setEventKey(null)
                .setData(data)
                .setFormat(NakadiRecord.Format.AVRO.getFormat());
    }

}
