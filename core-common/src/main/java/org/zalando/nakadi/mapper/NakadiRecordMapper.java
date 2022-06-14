package org.zalando.nakadi.mapper;

import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.RawMessageDecoder;
import org.apache.avro.message.RawMessageEncoder;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.generated.avro.EnvelopeV0;
import org.zalando.nakadi.generated.avro.MetadataV0;
import org.zalando.nakadi.generated.avro.PublishingBatchV0;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Service
public class NakadiRecordMapper {

    public static final String HEADER_FORMAT = new String(Format.AVRO.format);

    public enum Format {
        // the hope is that it is enough values from 0xFF till 0x7F (127) to cover possible.
        // the format comes as a first byte in kafka record so it could intersect with JSON
        AVRO(new byte[]{(byte) 0xFF});

        private final byte[] format;

        Format(final byte[] format) {
            this.format = format;
        }

        public byte[] getFormat() {
            return this.format;
        }
    }

    private final LocalSchemaRegistry localSchemaRegistry;
    private final RawMessageEncoder<EnvelopeV0> encoder;
    private final Map<String, RawMessageDecoder<PublishingBatchV0>> batchDecoders;
    private final Map<String, RawMessageDecoder<EnvelopeV0>> envelopeDecoders;
    private final byte latestEnvelopeVersion;

    public NakadiRecordMapper(final LocalSchemaRegistry localSchemaRegistry) {
        this.localSchemaRegistry = localSchemaRegistry;
        this.encoder = new RawMessageEncoder<>(new EnvelopeV0().getSpecificData(), EnvelopeV0.SCHEMA$);
        this.envelopeDecoders = new HashMap<>(2);
        this.batchDecoders = new HashMap<>(2);
        this.localSchemaRegistry.getEventTypeSchemaVersions(LocalSchemaRegistry.BATCH_PUBLISHING_KEY)
                .entrySet().forEach(entry -> {
                    envelopeDecoders.put(entry.getKey(), new RawMessageDecoder<>(
                            new EnvelopeV0().getSpecificData(), entry.getValue().getField("events").schema()
                            .getElementType(), EnvelopeV0.SCHEMA$));

                    batchDecoders.put(entry.getKey(), new RawMessageDecoder<>(
                            new PublishingBatchV0().getSpecificData(), entry.getValue(), PublishingBatchV0.SCHEMA$));
                }
        );
        this.latestEnvelopeVersion = Byte.valueOf(this.localSchemaRegistry
                .getLatestEventTypeSchemaVersion(LocalSchemaRegistry.BATCH_PUBLISHING_KEY)
                .getVersion());
    }

    public List<NakadiRecord> fromBytesBatch(final InputStream batch, final String batchVersion) {
        final RawMessageDecoder<PublishingBatchV0> decoder = batchDecoders.get(batchVersion);
        if (decoder == null) {
            throw new RuntimeException("unsupported batch version");
        }

        final PublishingBatchV0 publishingBatch = decoder.decode(batch, new PublishingBatchV0());

        final List<NakadiRecord> records = new LinkedList<>();
        for (final EnvelopeV0 envelope : publishingBatch.getEvents()) {
            records.add(new NakadiRecord()
                    .setMetadata(mapToNakadiMetadata(envelope.getMetadata()))
                    .setPayload(envelope.getPayload().array()));
        }

        return records;
    }

    public EnvelopeV0 fromBytesEnvelope(final InputStream data, final String envelopeVersion) {
        final RawMessageDecoder<EnvelopeV0> decoder = envelopeDecoders.get(envelopeVersion);
        if (decoder == null) {
            throw new RuntimeException(String.format("unsupported envelope version %s", envelopeVersion));
        }
        return decoder.decode(data, new EnvelopeV0());
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

    public ProducerRecord<byte[], byte[]> mapToProducerRecord(
            final NakadiRecord nakadiRecord,
            final String topic) throws IOException {
        final var partition = nakadiRecord.getMetadata().getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;

        final EnvelopeV0 env = mapToEnvelope(nakadiRecord);
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        baos.write(Format.AVRO.format);
        baos.write(latestEnvelopeVersion);
        encoder.encode(env, baos);

        return new ProducerRecord<>(topic, partitionInt, nakadiRecord.getEventKey(), baos.toByteArray());
    }

    private EnvelopeV0 mapToEnvelope(final NakadiRecord nakadiRecord) {
        final NakadiMetadata nakadiMetadata = nakadiRecord.getMetadata();
        final MetadataV0 metadata = MetadataV0.newBuilder()
                .setEid(nakadiMetadata.getEid())
                .setEventType(nakadiMetadata.getEventType())
                .setFlowId(nakadiMetadata.getFlowId())
                .setOccurredAt(nakadiMetadata.getOccurredAt())
                .setParentEids(nakadiMetadata.getParentEids())
                .setPartition(nakadiMetadata.getPartition())
                .setPartitionCompactionKey(nakadiMetadata.getPartitionCompactionKey())
                .setReceivedAt(nakadiMetadata.getReceivedAt())
                .setVersion(nakadiMetadata.getSchemaVersion())
                .setEventOwner(nakadiMetadata.getEventOwner())
                .setSpanCtx(nakadiMetadata.getSpanCtx())
                .setPublishedBy(nakadiMetadata.getPublishedBy())
                .setPartitionKeys(nakadiMetadata.getPartitionKeys())
                .build();
        return EnvelopeV0.newBuilder()
                .setMetadata(metadata)
                .setPayload(ByteBuffer.wrap(nakadiRecord.getPayload()))
                .build();
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
                .setPayload(payloadOutputStream.toByteArray());
    }

}
