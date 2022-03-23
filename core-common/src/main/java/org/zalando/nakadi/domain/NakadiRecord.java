package org.zalando.nakadi.domain;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.avro.io.EncoderFactory;

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

    private final String eventType;
    private final Integer partition;
    private final byte[] eventKey;
    private final byte[] data;
    private final byte[] format;

    public NakadiRecord(
            final String eventType,
            final Integer partition,
            final byte[] eventKey,
            final byte[] data,
            final byte[] format) {
        this.eventType = eventType;
        this.partition = partition;
        this.eventKey = eventKey;
        this.data = data;
        this.format = format;
    }

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

    public static NakadiRecord fromAvro(final String eventTypeName,
                                        final byte metadataVersion,
                                        final GenericRecord metadata,
                                        final IndexedRecord event) throws IOException {

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
        return new NakadiRecord(
                eventTypeName,
                null,
                null,
                data,
                NakadiRecord.Format.AVRO.getFormat());
    }

}
