package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class NakadiRecord implements Resource<NakadiRecord> {

    public static final String HEADER_FORMAT = new String(new byte[]{0});

    // todo maybe use a dedicated resource object
    @Override
    public String getName() {
        return metadata.getEid();
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

    private byte[] eventKey;
    private byte[] data;
    private byte[] format;
    private EventOwnerHeader owner;
    private NakadiAvroMetadata metadata;
    private String partition;

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

    public NakadiAvroMetadata getMetadata() {
        return metadata;
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
}
