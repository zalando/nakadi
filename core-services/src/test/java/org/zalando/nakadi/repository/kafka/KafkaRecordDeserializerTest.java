package org.zalando.nakadi.repository.kafka;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.message.BinaryMessageEncoder;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.io.Resource;
import org.zalando.nakadi.domain.EventTypeSchemaBase;
import org.zalando.nakadi.generated.avro.Envelope;
import org.zalando.nakadi.generated.avro.Metadata;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.nakadi.service.SchemaProviderService;
import org.zalando.nakadi.service.TestSchemaProviderService;
import org.zalando.nakadi.util.AvroUtils;
import org.zalando.nakadi.utils.TestUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;

public class KafkaRecordDeserializerTest {

    private final LocalSchemaRegistry localSchemaRegistry;
    private final SchemaProviderService schemaService;
    private final NakadiRecordMapper nakadiRecordMapper;
    private final Schema schema = AvroUtils.getParsedSchema(new DefaultResourceLoader()
            .getResource("test.deserialize.avro.avsc").getInputStream());
    private final KafkaRecordDeserializer deserializer;

    public KafkaRecordDeserializerTest() throws IOException {
        // FIXME: doesn't work without the trailing slash
        final Resource eventTypeRes = new DefaultResourceLoader().getResource("avro-schema/");
        localSchemaRegistry = TestUtils.getLocalSchemaRegistry();
        schemaService = new TestSchemaProviderService(localSchemaRegistry);
        nakadiRecordMapper = new NakadiRecordMapper(localSchemaRegistry);
        final SchemaProviderService singleSchemaProvider = new SchemaProviderService() {
            @Override
            public Schema getAvroSchema(final String etName, final String version) {
                return schema;
            }

            @Override
            public String getAvroSchemaVersion(final String etName, final Schema schema) {
                return null;
            }

            @Override
            public String getSchemaVersion(final String name, final String schema,
                                           final EventTypeSchemaBase.Type type) {
                return null;
            }
        };
        deserializer = new KafkaRecordDeserializer(nakadiRecordMapper, singleSchemaProvider);
    }

    @Test
    public void testDeserializeAvroNullEventInLogCompactedEventType() {
        final KafkaRecordDeserializer deserializer = new KafkaRecordDeserializer(nakadiRecordMapper, schemaService);

        Assert.assertNull(deserializer.deserializeToJsonBytes(null));
    }

    @Test
    public void testDeserializeAvroEnvelope() throws IOException {
        final ByteArrayOutputStream payload = new ByteArrayOutputStream();
        new GenericDatumWriter<>(schema).write(
                new GenericRecordBuilder(schema).set("foo", "bar").build(),
                EncoderFactory.get().directBinaryEncoder(payload, null));

        final String expectedOccurredAt = "2022-06-15T15:17:00Z";
        final Instant now = Instant.parse(expectedOccurredAt);
        final Envelope envelope = Envelope.newBuilder()
                .setMetadata(Metadata.newBuilder()
                        .setEid("4623130E-2983-4134-A472-F35154CFF980")
                        .setEventOwner("nakadi")
                        .setFlowId("xxx-event-flow-id")
                        .setOccurredAt(now)
                        .setEventType("nakadi-test-event-type")
                        .setVersion("1.0.0")
                        .build())
                .setPayload(ByteBuffer.wrap(payload.toByteArray()))
                .build();

        final ByteBuffer byteBuffer = Envelope.getEncoder().encode(envelope);
        final byte[] jsonBytes = deserializer.deserializeToJsonBytes(byteBuffer.array());

        final JSONObject event = new JSONObject(new String(jsonBytes));
        Assert.assertEquals("bar", event.get("foo"));
        Assert.assertEquals("4623130E-2983-4134-A472-F35154CFF980",
                event.getJSONObject("metadata").get("eid"));
        Assert.assertEquals(expectedOccurredAt,
                event.getJSONObject("metadata").get("occurred_at"));
    }

    @Test
    public void testDeserializeAvroSingleObjectEncoding() throws IOException {
        final ByteBuffer payload = new BinaryMessageEncoder<GenericRecord>(GenericData.get(), schema)
                .encode(new GenericRecordBuilder(schema).set("foo", "bar").build());

        final String expectedOccurredAt = "2022-06-15T15:17:00Z";
        final Instant now = Instant.parse(expectedOccurredAt);
        final Envelope envelope = Envelope.newBuilder()
                .setMetadata(Metadata.newBuilder()
                        .setEid("4623130E-2983-4134-A472-F35154CFF980")
                        .setEventOwner("nakadi")
                        .setFlowId("xxx-event-flow-id")
                        .setOccurredAt(now)
                        .setEventType("nakadi-test-event-type")
                        .setVersion("1.0.0")
                        .build())
                .setPayload(ByteBuffer.wrap(payload.array()))
                .build();

        final ByteBuffer byteBuffer = Envelope.getEncoder().encode(envelope);
        final byte[] jsonBytes = deserializer.deserializeToJsonBytes(byteBuffer.array());

        final JSONObject event = new JSONObject(new String(jsonBytes));
        Assert.assertEquals("bar", event.get("foo"));
        Assert.assertEquals("4623130E-2983-4134-A472-F35154CFF980",
                event.getJSONObject("metadata").get("eid"));
        Assert.assertEquals(expectedOccurredAt,
                event.getJSONObject("metadata").get("occurred_at"));
    }

}
