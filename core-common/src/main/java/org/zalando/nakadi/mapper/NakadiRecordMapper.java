package org.zalando.nakadi.mapper;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryDecoder;
import org.apache.avro.io.DecoderFactory;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.specific.SpecificData;
import org.apache.avro.specific.SpecificDatumReader;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Service
public class NakadiRecordMapper {

    private final LocalSchemaRegistry localSchemaRegistry;
    private final Map<String, RawMessageDecoder<PublishingBatchV0>> decoders;

    public NakadiRecordMapper(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
        this.decoders = new HashMap<>(2);
        this.localSchemaRegistry.getEventTypeSchemaVersions(LocalSchemaRegistry.BATCH_PUBLISHING_KEY)
                .entrySet().forEach(entry ->
                decoders.put(entry.getKey(), new RawMessageDecoder<>(
                        new SpecificData(), entry.getValue(), PublishingBatchV0.SCHEMA$)));
    }

    public List<NakadiRecord> fromBytesBatch(final byte[] batch, final byte batchVersion) {
        final RawMessageDecoder<PublishingBatchV0> decoder = decoders.get(String.valueOf(batchVersion));
        if (decoder == null) {
            throw new RuntimeException("unsupported batch version");
        }

        final PublishingBatchV0 publishingBatch = decoder.decode(
                new ByteArrayInputStream(batch), new PublishingBatchV0());

        final List<NakadiRecord> records = new LinkedList<>();
        for (final EnvelopeV0 envelope : publishingBatch.getEvents()) {
            records.add(new NakadiRecord()
                    .setMetadata(mapToNakadiMetadata(envelope.getMetadata()))
                    .setPayload(envelope.getPayload().array()));
        }

        return records;
    }

    private PublishingBatchV0 decode(final InputStream is,
                                     final Schema writerSchema) {
        final BinaryDecoder decoder = DecoderFactory.get().directBinaryDecoder(is, null);
        final SpecificDatumReader<PublishingBatchV0> reader = new SpecificDatumReader<>(
                writerSchema, PublishingBatchV0.SCHEMA$);
        try {
            return reader.read(null, decoder);
        } catch (IOException e) {
            throw new RuntimeException("can not read avro batch record", e);
        }
    }

    public NakadiMetadata mapToNakadiMetadata(final MetadataV0 metadata) {
        final NakadiMetadata nakadiMetadata = new NakadiMetadata();
        nakadiMetadata.setEid(metadata.getEid());
        nakadiMetadata.setEventType(metadata.getEventType());
        nakadiMetadata.setPartition(metadata.getPartition());
        nakadiMetadata.setOccurredAt(metadata.getOccurredAt());
        nakadiMetadata.setPublishedBy(metadata.getPublishedBy());
        nakadiMetadata.setReceivedAt(metadata.getReceivedAt());
        nakadiMetadata.setFlowId(metadata.getFlowId());
        nakadiMetadata.setSchemaVersion(metadata.getVersion());
        nakadiMetadata.setPartitionKeys(metadata.getPartitionKeys());
        nakadiMetadata.setPartitionCompactionKey(metadata.getPartitionCompactionKey());
        nakadiMetadata.setParentEids(metadata.getParentEids());
        nakadiMetadata.setSpanCtx(metadata.getSpanCtx());

        return nakadiMetadata;
    }

    public static ProducerRecord<byte[], byte[]> mapToProducerRecord(
            final NakadiRecord nakadiRecord,
            final String topic) throws IOException {

        final var partition = nakadiRecord.getMetadata().getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;

        // [envelope_version_byte] [envelope]

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write((byte) 0);

        final EnvelopeV0 env = null;
        EnvelopeV0.getEncoder().encode(env, baos);

        return new ProducerRecord<>(topic, partitionInt, nakadiRecord.getEventKey(), baos.toByteArray());
    }

    public NakadiRecord fromAvroGenericRecord(final NakadiMetadata metadata,
                                              final GenericRecord event) throws IOException {

        final var payloadOutputStream = new ByteArrayOutputStream();
        final var eventWriter = new GenericDatumWriter(event.getSchema());
        eventWriter.write(event, EncoderFactory.get()
                .directBinaryEncoder(payloadOutputStream, null));

        return new NakadiRecord()
                .setMetadata(metadata)
                .setEventKey(null) // fixme remove it once event key implemented
                .setPayload(payloadOutputStream.toByteArray())
                .setFormat(NakadiRecord.Format.AVRO.getFormat());
    }

}
