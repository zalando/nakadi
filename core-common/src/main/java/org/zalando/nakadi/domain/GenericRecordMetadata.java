package org.zalando.nakadi.domain;

import org.apache.avro.generic.GenericRecord;

public class GenericRecordMetadata implements NakadiMetadata {

    private final GenericRecord metadata;

    public GenericRecordMetadata(final GenericRecord metadata) {
        this.metadata = metadata;
    }

    public String getEid() {
        return metadata.get("eid").toString();
    }

    public String getEventType() {
        return metadata.get("event_type").toString();
    }

    public String getPartition() {
        return metadata.get("partition").toString();
    }


}
