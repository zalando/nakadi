package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.avro.AvroMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.EncoderFactory;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EnvelopeHolder;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.AvroSchema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

public class KafkaRecordDeserializerTest {

    private final AvroSchema avroSchema;
    private static final long SOME_TIME = 1643290232172l;
    private static final String SOME_TIME_DATE_STRING = "2022-01-27T13:30:32.172Z";

    public KafkaRecordDeserializerTest() throws IOException {
        // FIXME: doesn't work without the trailing slash
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("event-type-schema/");
        avroSchema = new AvroSchema(new AvroMapper(), new ObjectMapper(), eventTypeRes);
    }

    @Test
    public void testDeserializeAvro() throws IOException {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(avroSchema);

        final JSONObject jsonObject = new JSONObject()
                .put("flow_id", "hek")
                .put("partition", 0)
                .put("published_by", "nakadi-test");

        // prepare the same bytes as we would put in Kafka record
        final byte[] data0 = EnvelopeHolder.produceBytes(
                (byte) 1, // metadata version
                getMetadataWriter("1", "0", jsonObject),
                getEventWriter0());

        final byte[] data1 = EnvelopeHolder.produceBytes(
                (byte) 0, // metadata version
                getMetadataWriter("0", "1", jsonObject),
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

        Assert.assertTrue(
                getExpectedNode0(null).similar(new JSONObject(new String(deserializedEvent0))));

        Assert.assertTrue(
                getExpectedNode1().similar(new JSONObject(new String(deserializedEvent1))));
    }

    @Test
    public void testDeserializeAvroMetadata0() throws IOException {
        final var metadataVersion = "0";

        final JSONObject jsonObject = new JSONObject()
                .put("flow_id", "hek")
                .put("partition", 0)
                .put("published_by", "nakadi-test");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getExpectedNode0(jsonObject);
        Assert.assertTrue(expectedJson.similar(actualJson));
    }

    @Test
    public void testDeserializeAvroMetadata1() throws IOException {
        final var metadataVersion = "1";

        final JSONObject jsonObject = new JSONObject()
                .put("flow_id", "hek")
                .put("partition", 0)
                .put("published_by", "nakadi-test");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getExpectedNode0(jsonObject);
        Assert.assertTrue(expectedJson.similar(actualJson));
    }

    @Test
    public void testDeserializeAvroMetadata2() throws IOException {
        final var metadataVersion = "2";
        final JSONObject jsonObject = new JSONObject()
                .put("flow_id", "hek")
                .put("partition", "0")
                .put("published_by", "nakadi-test");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getExpectedNode0(jsonObject);
        Assert.assertTrue(expectedJson.similar(actualJson));
    }

    @Test
    public void testDeserializeAvroMetadata3WithoutDefaults() throws IOException {
        final var metadataVersion = "3";

        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("partition", "0");

        //changed to optional but always filled by nakadi
        jsonObject.put("published_by", "nakadi-test");
        jsonObject.put("flow_id", "hek");

        //new optional fields
        jsonObject.put("partition_keys", List.of("1","2"));
        jsonObject.put("parent_eids", List.of(
                "32f5dae5-4fc4-4cda-be07-b313b58490ac",
                "32f5dae5-4fc4-4cda-be07-b313b58490ad")
        );
        jsonObject.put("span_ctx", "sek");
        jsonObject.put("partition_compaction_key", "some_key");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getExpectedNode0(jsonObject);

        Assert.assertTrue(expectedJson.similar(actualJson));
    }

    @Test
    public void testDeserializeAvroMetadata3WithDefaults() throws IOException {
        final var metadataVersion = "3";

        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("partition", "0");
        //changed to optional but always filled by nakadi
        jsonObject.put("published_by", "nakadi-test");
        jsonObject.put("flow_id", "hek");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getExpectedNode0(jsonObject);
        Assert.assertTrue(expectedJson.similar(actualJson));
    }

    @Test
    public void testMetadataJsonHasOnlyNonNullValues() throws IOException {
        final var metadataVersion = "3";

        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("partition", "0");
        //changed to optional but always filled by nakadi
        jsonObject.put("published_by", "nakadi-test");
        jsonObject.put("flow_id", "hek");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);

        final var metadataObj = actualJson.getJSONObject("metadata");

        final List<String> nullKeys = new ArrayList<>();
        metadataObj.toMap().forEach((k, v) -> {
            if(v == null){
                nullKeys.add(k);
            }
        });

        Assert.assertTrue("No keys with null values should be present in metadata", nullKeys.isEmpty());
    }

    @Test
    public void testDeserializeAvroMetadata4WithDefaults() throws IOException {
        final var metadataVersion = "4";

        final JSONObject jsonObject = new JSONObject();
        jsonObject.put("partition", "1");
        jsonObject.put("published_by", "nakadi-test");
        jsonObject.put("flow_id", "hek");

        final var actualJson = getSerializedJsonObject(metadataVersion, jsonObject);
        final var expectedJson = getBaseExpectedNode(metadataVersion, "0", jsonObject);

        Assert.assertTrue(expectedJson.similar(actualJson));
    }


    private JSONObject getSerializedJsonObject(final String metadataVersion,
                                               final JSONObject metadataOverride) throws IOException {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(avroSchema);

        final var eventWriter = getEventWriter0();

        // prepare the same bytes as we would put in Kafka record
        final byte[] data = EnvelopeHolder.produceBytes(
                Byte.parseByte(metadataVersion),
                getMetadataWriter(metadataVersion, "0", metadataOverride),
                eventWriter);

        // try to deserialize that data when we would read Kafka record
        final byte[] deserializedEvent = deserializer.deserialize(
                NakadiRecord.Format.AVRO.getFormat(),
                data
        );

        return new JSONObject(new String(deserializedEvent));
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

    private String getSchemaVersionFieldName(final String metadataVersion) {
        // well, f*ck
        return Integer.parseInt(metadataVersion) < 4 ? "schema_version" : "version";
    }

    private EnvelopeHolder.EventWriter getMetadataWriter(final String metadataVersion,
                                                         final String schemaVersion,
                                                         final JSONObject metadataOverride) {
        return os -> {
            final GenericRecord metadata =
                    new GenericData.Record(avroSchema.getEventTypeSchema(AvroSchema.METADATA_KEY, metadataVersion));

            metadata.put("occurred_at", SOME_TIME);
            metadata.put("received_at", SOME_TIME);
            metadata.put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab");
            metadata.put("event_type", "nakadi.access.log");

            metadata.put(getSchemaVersionFieldName(metadataVersion), schemaVersion);

            Optional.ofNullable(metadataOverride).ifPresent(fn ->
                metadataOverride.toMap().forEach(metadata::put)
            );

            final GenericDatumWriter eventWriter = new GenericDatumWriter(metadata.getSchema());
            eventWriter.write(metadata, EncoderFactory.get()
                    .directBinaryEncoder(os, null));
        };
    }

    private JSONObject getBaseExpectedNode(final String metadataVersion,
                                           final String schemaVersion,
                                           final JSONObject metadataOverride) {
        final JSONObject metadata = new JSONObject().
                 put("occurred_at", SOME_TIME_DATE_STRING)
                .put("eid", "32f5dae5-4fc4-4cda-be07-b313b58490ab")
                .put("flow_id", "hek")
                .put("received_at", SOME_TIME_DATE_STRING)
                .put(getSchemaVersionFieldName(metadataVersion), schemaVersion)
                .put("published_by", "nakadi-test")
                .put("event_type", "nakadi.access.log")
                .put("partition", 0);

        Optional.ofNullable(metadataOverride).ifPresent(fn -> {
            final var iterator = metadataOverride.keys();
            while (iterator.hasNext()) {
                final var key = iterator.next();
                metadata.put(key, metadataOverride.get(key));
            }
        });

        final JSONObject event = new JSONObject()
                .put("method", "POST")
                .put("path", "/event-types")
                .put("query", "")
                .put("app", "nakadi")
                .put("app_hashed", "hashed-app")
                .put("status_code", 201)
                .put("response_time_ms", 10)
                .put("accept_encoding", "-")
                .put("content_encoding", "--")
                .put("metadata", metadata);
        return event;
    }

    private JSONObject getExpectedNode0(final JSONObject metadataOverride) {
        return getBaseExpectedNode("0", "0", metadataOverride);
    }

    private JSONObject getExpectedNode1() {
        final JSONObject event = getBaseExpectedNode("1", "1", null);
        event.put("user_agent", "test-user-agent");
        event.put("request_length", 111);
        event.put("response_length", 222);
        return event;
    }
}
