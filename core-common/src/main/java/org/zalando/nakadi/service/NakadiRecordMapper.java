package org.zalando.nakadi.service;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiAvroMetadata;
import org.zalando.nakadi.domain.NakadiRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@Service
public class NakadiRecordMapper {

    private final AvroSchema avroSchema;

    public NakadiRecordMapper(final AvroSchema avroSchema) {
        this.avroSchema = avroSchema;
    }

    public List<NakadiRecord> fromBytesBatch(final byte[] batch) throws IOException {
        final List<NakadiRecord> records = new LinkedList<>();
        final ByteBuffer tmp = ByteBuffer.wrap(batch);
        while (tmp.hasRemaining()) {
            final int recordStart = tmp.position();
            final byte metadataVersion = tmp.get();
            final int metadataLength = tmp.getInt();
            final byte[] metadata = new byte[metadataLength];
            tmp.get(metadata);

            final int payloadLength = tmp.getInt();
            final byte[] wholeRecord = new byte[1 + 4 + metadataLength + 4 + payloadLength];
            tmp.position(recordStart);
            tmp.get(wholeRecord);

            final Schema metadataSchema = avroSchema.getEventTypeSchema(
                    AvroSchema.METADATA_KEY, Byte.toString(metadataVersion));
            records.add(new NakadiRecord()
                    .setMetadata(new NakadiAvroMetadata(metadataVersion, metadataSchema, metadata))
                    .setData(wholeRecord));
        }

        return records;
    }

    public NakadiRecord fromAvroGenericRecord(final NakadiAvroMetadata metadata,
                                              final GenericRecord event) throws IOException {
        final byte[] data = EnvelopeHolder.produceBytes(
                metadata.getMetadataVersion(),
                metadata,
                (outputStream -> {
                    final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
                    eventWriter.write(event, EncoderFactory.get()
                            .directBinaryEncoder(outputStream, null));
                }));
        return new NakadiRecord()
                .setMetadata(metadata)
                .setEventKey(null) // fixme remove it once event key implemented
                .setData(data)
                .setFormat(NakadiRecord.Format.AVRO.getFormat());
    }

    public ProducerRecord<byte[], byte[]> toProducerRecord(
            final String topic, final NakadiRecord nakadiRecord) {

        final var partition = nakadiRecord.getMetadata().getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;
        final var eventKey = nakadiRecord.getEventKey();
       
        // TODO - Re serialize data with the enriched metadata
        final var data = nakadiRecord.getData();

        return new ProducerRecord<>(topic, partitionInt, eventKey, data);
    }

}
