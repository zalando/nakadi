package org.zalando.nakadi.domain;

import org.apache.avro.generic.GenericRecord;

public class GenericRecordMetadata implements NakadiMetadata {

    private final GenericRecord metadata;
    private final byte metadataVersion;

    public GenericRecordMetadata(final GenericRecord metadata,
                                 final byte metadataVersion) {
        this.metadata = metadata;
        this.metadataVersion = metadataVersion;
    }

    public byte getMetadataVersion() {
        return metadataVersion;
    }

    public String getEid() {
        return metadata.get("eid").toString();
    }

    public String getEventType() {
        return metadata.get("event_type").toString();
    }

    public String getPartitionStr() {
        final Object partition = metadata.get("partition");
        if (partition == null) {
            return null;
        }
        return partition.toString();
    }

    public Integer getPartitionInt() {
        final Object partition = metadata.get("partition");
        if (partition == null) {
            return null;
        }
        return Integer.valueOf(partition.toString());
    }


}
