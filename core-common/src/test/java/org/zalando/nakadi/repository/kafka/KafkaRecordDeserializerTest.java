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

public class KafkaRecordDeserializerTest {

    private final AvroSchema avroSchema;

    public KafkaRecordDeserializerTest() throws IOException {
        final Resource metadataRes = new DefaultResourceLoader().getResource("event-type-schema/metadata.avsc");
        // FIXME: doesn't work without the trailing slash
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), metadataRes, eventTypeRes);
    }

    @Test
    public void testDeserializeAvro() throws IOException {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(avroSchema, "nakadi.access.log");

        // prepare the same bytes as we would put in Kafka record
        final byte[] data0 = EnvelopeHolder.produceBytes(
                AvroSchema.METADATA_VERSION,
                getMetadataWriter("0"),
                getEventWriter0());

        final byte[] data1 = EnvelopeHolder.produceBytes(
                AvroSchema.METADATA_VERSION,
                getMetadataWriter("1"),
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
                expectedJsonEvent1(),
                new ObjectMapper()
                        .readValue(deserializedEvent1, ObjectNode.class));
    }

    private EnvelopeHolder.EventWriter getEventWriter0() {
        return os -> {
            final GenericRecord event = new GenericData.Record(
                    avroSchema.getEventTypeSchema("nakadi.access.log", "0"));
            event.put("method", "POST");
            event.put("path", "/event-types");
            event.put("query", "");
            event.put("app", "nakadi");
            event.put("app_hashed", "hashed-app");
            event.put("status_code", 201);
            event.put("response_time_ms", 10);
            event.put("accept_encoding", "-");
            event.put("content_encoding", "--");

            final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
            eventWriter.write(event, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private EnvelopeHolder.EventWriter getEventWriter1() {
        return os -> {
            final GenericRecord event = new GenericData.Record(
                    avroSchema.getEventTypeSchema("nakadi.access.log", "1"));
            event.put("method", "POST");
            event.put("path", "/event-types");
            event.put("query", "");
            event.put("user_agent", "test-user-agent");
            event.put("app", "nakadi");
            event.put("app_hashed", "hashed-app");
            event.put("status_code", 201);
            event.put("response_time_ms", 10);
            event.put("accept_encoding", "-");
            event.put("content_encoding", "--");

            final GenericDatumWriter eventWriter = new GenericDatumWriter(event.getSchema());
            eventWriter.write(event, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private EnvelopeHolder.EventWriter getMetadataWriter(final String schemaVersion) {
        return os -> {
            final GenericRecord metadata = new GenericData.Record(avroSchema.getMetadataSchema());
            final long someEqualTime = 1643290232172l;
            metadata.put("occurred_at", someEqualTime);
            metadata.put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab");
            metadata.put("flow_id", "hek");
            metadata.put("event_type", "test-et-name");
            metadata.put("partition", 0);
            metadata.put("received_at", someEqualTime);
            metadata.put("schema_version", schemaVersion);
            metadata.put("published_by", "nakadi-test");

            final GenericDatumWriter eventWriter = new GenericDatumWriter(metadata.getSchema());
            eventWriter.write(metadata, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private ObjectNode expectedJsonEvent1() {
        final ObjectMapper mapper = new ObjectMapper();
        final ObjectNode metadata = mapper.createObjectNode()
                .put("occurred_at", "2022-01-27T13:30:32.172Z")
                .put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab")
                .put("flow_id", "hek")
                .put("received_at", "2022-01-27T13:30:32.172Z")
                .put("schema_version", "1")
                .put("published_by", "nakadi-test")
                .put("event_type", "test-et-name")
                .put("partition", 0);
        final ObjectNode event = mapper.createObjectNode()
                .put("method", "POST")
                .put("path", "/event-types")
                .put("query", "")
                .put("user_agent", "test-user-agent")
                .put("app", "nakadi")
                .put("app_hashed", "hashed-app")
                .put("status_code", 201)
                .put("response_time_ms", 10)
                .put("accept_encoding", "-")
                .put("content_encoding", "--")
                .set("metadata", metadata);
        return event;
    }
}
