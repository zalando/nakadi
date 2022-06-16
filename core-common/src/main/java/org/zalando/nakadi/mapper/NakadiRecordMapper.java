package org.zalando.nakadi.mapper;

import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.NakadiMetadata;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.exceptions.runtime.AvroPayloadDecodingException;
import org.zalando.nakadi.exceptions.runtime.NakadiRuntimeException;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.generated.avro.PublishingBatch;
import org.zalando.nakadi.service.LocalSchemaRegistry;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@Service
public class NakadiRecordMapper {

    public static final byte[] AVRO_FORMAT = new byte[]{(byte) 0x0};
    public static final String HEADER_FORMAT = new String(AVRO_FORMAT);

    public NakadiRecordMapper(final LocalSchemaRegistry localSchemaRegistry) {
        localSchemaRegistry.getEventTypeSchemaVersions(LocalSchemaRegistry.BATCH_PUBLISHING_KEY)
                .entrySet().forEach(entry -> {
                    PublishingBatch.getDecoder().addSchema(entry.getValue());
                    Envelope.getDecoder().addSchema(entry.getValue()
                            .getField("events").schema().getElementType());
                }
        );
    }

    public List<NakadiRecord> fromBytesBatch(final InputStream batch) {
        final PublishingBatch publishingBatch;
        try {
            publishingBatch = PublishingBatch.getDecoder()
                    .decode(batch, new PublishingBatch());
        } catch (AvroRuntimeException are) {
            throw new AvroPayloadDecodingException("failed to decode envelope", are);
        } catch (IOException e) {
            throw new NakadiRuntimeException("failed to decode publishing batch", e);
        }

        final List<NakadiRecord> records = new LinkedList<>();
        for (final Envelope envelope : publishingBatch.getEvents()) {
            records.add(new NakadiRecord()
                    .setMetadata(mapToNakadiMetadata(envelope.getMetadata()))
                    .setPayload(envelope.getPayload().array()));
        }

        return records;
    }

    public Envelope fromBytesEnvelope(final InputStream data) {
        try {
            return Envelope.getDecoder().decode(data, new Envelope());
        } catch (AvroRuntimeException are) {
            throw new AvroPayloadDecodingException("failed to decode envelope", are);
        } catch (IOException io) {
            throw new NakadiRuntimeException("failed to decode envelope", io);
        }
    }

    public NakadiMetadata mapToNakadiMetadata(final Metadata metadata) {
        final NakadiMetadata nakadiMetadata = new NakadiMetadata();
        nakadiMetadata.setEid(metadata.getEid());
        nakadiMetadata.setEventType(metadata.getEventType());
        nakadiMetadata.setEventOwner(metadata.getEventOwner());
        nakadiMetadata.setFlowId(metadata.getFlowId());
        nakadiMetadata.setOccurredAt(metadata.getOccurredAt());
        nakadiMetadata.setPartition(metadata.getPartition());
        nakadiMetadata.setParentEids(metadata.getParentEids());
        nakadiMetadata.setPartitionCompactionKey(metadata.getPartitionCompactionKey());
        nakadiMetadata.setPartitionKeys(metadata.getPartitionKeys());
        nakadiMetadata.setPublishedBy(metadata.getPublishedBy());
        nakadiMetadata.setReceivedAt(metadata.getReceivedAt());
        nakadiMetadata.setSchemaVersion(metadata.getVersion());
        nakadiMetadata.setSpanCtx(metadata.getSpanCtx());

        return nakadiMetadata;
    }

    public ProducerRecord<byte[], byte[]> mapToProducerRecord(
            final NakadiRecord nakadiRecord,
            final String topic) throws IOException {
        final var partition = nakadiRecord.getMetadata().getPartition();
        final var partitionInt = (partition != null) ? Integer.valueOf(partition) : null;

        final Envelope env = mapToEnvelope(nakadiRecord);
        final ByteBuffer byteBuffer = Envelope.getEncoder().encode(env);

        return new ProducerRecord<>(topic, partitionInt, nakadiRecord.getEventKey(), byteBuffer.array());
    }

    private Envelope mapToEnvelope(final NakadiRecord nakadiRecord) {
        final NakadiMetadata nakadiMetadata = nakadiRecord.getMetadata();
        final Metadata metadata = Metadata.newBuilder()
                .setEid(nakadiMetadata.getEid())
                .setEventType(nakadiMetadata.getEventType())
                .setEventOwner(nakadiMetadata.getEventOwner())
                .setFlowId(nakadiMetadata.getFlowId())
                .setOccurredAt(nakadiMetadata.getOccurredAt())
                .setPartition(nakadiMetadata.getPartition())
                .setParentEids(nakadiMetadata.getParentEids())
                .setPartitionCompactionKey(nakadiMetadata.getPartitionCompactionKey())
                .setPartitionKeys(nakadiMetadata.getPartitionKeys())
                .setPublishedBy(nakadiMetadata.getPublishedBy())
                .setReceivedAt(nakadiMetadata.getReceivedAt())
                .setSpanCtx(nakadiMetadata.getSpanCtx())
                .setVersion(nakadiMetadata.getSchemaVersion())
                .build();
        return Envelope.newBuilder()
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
