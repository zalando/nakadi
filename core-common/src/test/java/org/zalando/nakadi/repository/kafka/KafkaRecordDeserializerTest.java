package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

public class KafkaRecordDeserializerTest {

    private final AvroSchema avroSchema;
    private static final long SOME_TIME = 1643290232172l;

    public KafkaRecordDeserializerTest() throws IOException {
        // FIXME: doesn't work without the trailing slash
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
    }

    @Test
    public void testDeserializeAvro() throws IOException {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(avroSchema);
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {
            gr.put("flow_id", "hek");
            gr.put("partition", 0);
            gr.put("received_at", SOME_TIME);
        };

        // prepare the same bytes as we would put in Kafka record
        final byte[] data0 = EnvelopeHolder.produceBytes(
                (byte) 1, // metadata version
                getMetadataWriter("1", "0", modifyMetadata),
                getEventWriter0());

        final byte[] data1 = EnvelopeHolder.produceBytes(
                (byte) 0, // metadata version
                getMetadataWriter("0", "1", modifyMetadata),
                getEventWriter1());

        // try to deserialize that data when we would read Kafka record
        final byte[] deserializedEvent0 = deserializer.deserialize(
                NakadiRecord.Format.AVRO.getFormat(),
                data0
        );
        final byte[] deserializedEvent1 = deserializer.deserialize(
                NakadiRecord.Format.AVRO.getFormat(),
                data1
        );

        Assert.assertEquals(
                getExpectedNode0(),
                new ObjectMapper()
                        .readValue(deserializedEvent0, ObjectNode.class));

        Assert.assertEquals(
                getExpectedNode1(),
                new ObjectMapper()
                        .readValue(deserializedEvent1, ObjectNode.class));
    }

    @Test
    public void testDeserializeAvroMetadata0() throws IOException {
        final var metadataVersion = "0";
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {
            gr.put("flow_id", "hek");
            gr.put("partition", 0);
            gr.put("received_at", SOME_TIME);
        };
        checkMetadataSerializeDeserialize(metadataVersion, modifyMetadata);
    }

    @Test
    public void testDeserializeAvroMetadata1() throws IOException {
        final var metadataVersion = "1";
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {
            gr.put("flow_id", "hek");
            gr.put("partition", 0);
            gr.put("received_at", SOME_TIME);
        };
        checkMetadataSerializeDeserialize(metadataVersion, modifyMetadata);
    }


    @Test
    public void testDeserializeAvroMetadata2() throws IOException {
        final var metadataVersion = "2";
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {
            gr.put("partition", "0");
            gr.put("flow_id", "hek");
            gr.put("received_at", SOME_TIME);
        };
        checkMetadataSerializeDeserialize(metadataVersion, modifyMetadata);
    }

    @Test
    public void testDeserializeAvroMetadata3WithoutDefaults() throws IOException {
        final var metadataVersion = "3";
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {

            //fields changed to optional
            gr.put("partition", "0");
            gr.put("flow_id", "hek");
            //optional but filled & required by nakadi
            gr.put("received_at", SOME_TIME);

            //new optional fields
            gr.put("partition_keys", List.of("1","2"));
            gr.put("parent_eids", List.of("1","2"));
            gr.put("span_ctx", "sek");
        };
        checkMetadataSerializeDeserialize(metadataVersion, modifyMetadata);
    }

    @Test
    public void testDeserializeAvroMetadata3WithDefaults() throws IOException {
        final var metadataVersion = "3";
        final Consumer<GenericRecord> modifyMetadata = (gr) -> {
            gr.put("partition", "0");
            //optional but filled & required by nakadi
            gr.put("received_at", SOME_TIME);
        };
        checkMetadataSerializeDeserialize(metadataVersion, modifyMetadata);
    }


    private void checkMetadataSerializeDeserialize(final String metadataVersion,
                                                   final Consumer<GenericRecord> modifyMetadata) throws IOException {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(avroSchema);
        final var eventWriter = getEventWriter1();
        // prepare the same bytes as we would put in Kafka record
        final byte[] data = EnvelopeHolder.produceBytes(
                Byte.parseByte(metadataVersion),
                getMetadataWriter(metadataVersion, "1", modifyMetadata),
                eventWriter);

        // try to deserialize that data when we would read Kafka record
        final byte[] deserializedEvent = deserializer.deserialize(
                NakadiRecord.Format.AVRO.getFormat(),
                data
        );
    }


    private GenericRecord getBaseRecord(final String schemaVersion) {
        final GenericRecord event = new GenericData.Record(
                avroSchema.getEventTypeSchema("nakadi.access.log", schemaVersion));
        event.put("method", "POST");
        event.put("path", "/event-types");
        event.put("query", "");
        event.put("app", "nakadi");
        event.put("app_hashed", "hashed-app");
        event.put("status_code", 201);
        event.put("response_time_ms", 10);
        event.put("accept_encoding", "-");
        event.put("content_encoding", "--");
        return event;
    }

    private EnvelopeHolder.EventWriter getEventWriter0() {
        return os -> {
            final GenericRecord event = getBaseRecord("0");

            final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
            eventWriter.write(event, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private EnvelopeHolder.EventWriter getEventWriter1() {
        return os -> {
            final GenericRecord event = getBaseRecord("1");
            event.put("user_agent", "test-user-agent");
            event.put("request_length", 111);
            event.put("response_length", 222);

            final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
            eventWriter.write(event, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private EnvelopeHolder.EventWriter getMetadataWriter(final String metadataVersion,
                                                         final String schemaVersion,
                                                         final Consumer<GenericRecord> overWrite) {
        return os -> {
            final GenericRecord metadata =
                    new GenericData.Record(avroSchema.getEventTypeSchema(AvroSchema.METADATA_KEY, metadataVersion));

            metadata.put("occurred_at", SOME_TIME);
            metadata.put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab");
            metadata.put("event_type", "nakadi.access.log");
            metadata.put("schema_version", schemaVersion);
            metadata.put("published_by", "nakadi-test");

            Optional.ofNullable(overWrite).ifPresent(fn -> fn.accept(metadata));

            final GenericDatumWriter eventWriter = new GenericDatumWriter(metadata.getSchema());
            eventWriter.write(metadata, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private ObjectNode getBaseExpectedNode(final String schemaVersion) {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode metadata = mapper.createObjectNode()
                .put("occurred_at", "2022-01-27T13:30:32.172Z")
                .put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab")
                .put("flow_id", "hek")
                .put("received_at", "2022-01-27T13:30:32.172Z")
                .put("schema_version", schemaVersion)
                .put("published_by", "nakadi-test")
                .put("event_type", "nakadi.access.log")
                .put("partition", 0);
        final ObjectNode event = mapper.createObjectNode()
                .put("method", "POST")
                .put("path", "/event-types")
                .put("query", "")
                .put("app", "nakadi")
                .put("app_hashed", "hashed-app")
                .put("status_code", 201)
                .put("response_time_ms", 10)
                .put("accept_encoding", "-")
                .put("content_encoding", "--")
                .set("metadata", metadata);
        return event;
    }

    private ObjectNode getExpectedNode0() {
        return getBaseExpectedNode("0");
    }

    private ObjectNode getExpectedNode1() {
        final ObjectNode event = getBaseExpectedNode("1");
        event.put("user_agent", "test-user-agent");
        event.put("request_length", 111);
        event.put("response_length", 222);
        return event;
    }
}
