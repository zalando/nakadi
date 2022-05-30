package org.zalando.nakadi.domain;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.zalando.nakadi.util.GenericRecordWrapper;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStream;

public class NakadiAvroMetadata extends NakadiMetadata {
    public static final String EID = "eid";
    public static final String OCCURRED_AT = "occurred_at";
    public static final String PUBLISHED_BY = "published_by";
    public static final String RECEIVED_AT = "received_at";
    public static final String EVENT_TYPE = "event_type";
    public static final String FLOW_ID = "flow_id";
    public static final String SCHEMA_VERSION = "version";
    public static final String PARTITION = "partition";
    public static final String PARTITION_KEYS = "partition_keys";
    public static final String PARTITION_COMPACTION_KEY = "partition_compaction_key";
    public static final String PARENT_EIDS = "parent_eids";
    public static final String SPAN_CTX = "span_ctx";

    private final Schema metadataAvroSchema;

    public NakadiAvroMetadata(
            final byte metadataVersion,
            final Schema metadataAvroSchema) {
        super(metadataVersion);
        this.metadataAvroSchema = metadataAvroSchema;
    }

    public NakadiAvroMetadata(
            final byte metadataVersion,
            final Schema metadataAvroSchema,
            final byte[] data) throws IOException {

        super(metadataVersion);
        this.metadataAvroSchema = metadataAvroSchema;

        final GenericDatumReader datumReader = new GenericDatumReader(metadataAvroSchema);
        final GenericRecord genericRecord = (GenericRecord) datumReader.read(null,
                DecoderFactory.get().directBinaryDecoder(
                        new ByteArrayInputStream(data), null));
        final GenericRecordWrapper wrapper = new GenericRecordWrapper(genericRecord);

        this.setEid(wrapper.getString(EID));
        this.setEventType(wrapper.getString(EVENT_TYPE));
        this.setPartition(wrapper.getString(PARTITION));
        this.setOccurredAt(wrapper.getLong(OCCURRED_AT));
        this.setPublishedBy(wrapper.getString(PUBLISHED_BY));
        this.setReceivedAt(wrapper.getLong(RECEIVED_AT));
        this.setFlowId(wrapper.getString(FLOW_ID));
        this.setSchemaVersion(wrapper.getString(SCHEMA_VERSION));
        this.setPartitionKeys(wrapper.getListOfStrings(PARTITION_KEYS));
        this.setPartitionCompactionKey(wrapper.getString(PARTITION_COMPACTION_KEY));
        this.setParentEids(wrapper.getListOfStrings(PARENT_EIDS));
        this.setSpanCtx(wrapper.getString(SPAN_CTX));
    }

    public Schema getMetadataAvroSchema() {
        return metadataAvroSchema;
    }

    @Override
    public void write(final OutputStream outputStream) throws IOException {
        final var metadata = new GenericRecordBuilder(metadataAvroSchema)
                .set(EID, getEid())
                .set(EVENT_TYPE, getEventType())
                .set(PARTITION, getPartition())
                .set(OCCURRED_AT, getOccurredAt())
                .set(PUBLISHED_BY, getPublishedBy())
                .set(RECEIVED_AT, getReceivedAt())
                .set(FLOW_ID, getFlowId())
                .set(SCHEMA_VERSION, getSchemaVersion())
                .set(PARTITION_KEYS, getPartitionKeys())
                .set(PARTITION_COMPACTION_KEY, getPartitionCompactionKey())
                .set(PARENT_EIDS, getParentEids())
                .set(SPAN_CTX, getSpanCtx())
                .build();
        final GenericDatumWriter eventWriter = new GenericDatumWriter(metadataAvroSchema);
        eventWriter.write(metadata, EncoderFactory.get()
                .directBinaryEncoder(outputStream, null));
    }
}
