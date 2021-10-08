package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Assert;
import org.junit.Test;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;
import org.zalando.nakadi.webservice.utils.TestStreamingClient;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static org.zalando.nakadi.utils.TestUtils.waitFor;

public class AvroAT extends BaseAT {

    @Test
    public void testAvroPublishedAndReceivedNakadiAccessLog() throws IOException {
        final EventType eventType = NakadiTestUtils.createEventType();
        NakadiTestUtils.publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        final Subscription sub = NakadiTestUtils.createSubscriptionForEventTypeFromBegin("nakadi.access.log");
        final TestStreamingClient client = TestStreamingClient
                .create(URL, sub.getId(), "batch_limit=1")
                .start();

        waitFor(() -> Assert.assertFalse(client.getBatches().isEmpty()), TimeUnit.SECONDS.toMillis(2), 100);
    }

    @Test
    public void testAvroRecordsInKafka() throws IOException {
        final EventType eventType = NakadiTestUtils.createEventType();
        NakadiTestUtils.publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        consumeAvro(consumerRecord -> {
            Assert.assertEquals(
                    new String(consumerRecord.headers().lastHeader(NakadiRecord.HEADER_EVENT_TYPE).value()),
                    "nakadi.access.log"
            );
            final GenericRecord record = deserializeGenericRecord(consumerRecord.value());
            final GenericRecord metadata = (GenericRecord) record.get("metadata");
            Assert.assertEquals(
                    "-",
                    record.get("app").toString());
            Assert.assertEquals(
                    "8062d40935e0c4cc1ff94735417620dea098c90af96a72de271b84e5fdde1040",
                    record.get("app_hashed").toString());
            Assert.assertEquals(
                    "avro/v1",
                    metadata.get("version").toString());
            Assert.assertEquals(
                    "nakadi.access.log",
                    metadata.get("event_type").toString());
        });
    }

    private void consumeAvro(final Consumer<ConsumerRecord<byte[], byte[]>> action)
            throws IOException {
        final String topic = (String) NakadiTestUtils
                .listTimelines("nakadi.access.log").get(0).get("topic");
        final KafkaConsumer<byte[], byte[]> consumer =
                new KafkaConsumer<>(createKafkaProperties());
        consumer.assign(ImmutableList.of(
                new TopicPartition(topic, 0),
                new TopicPartition(topic, 1),
                new TopicPartition(topic, 2),
                new TopicPartition(topic, 3),
                new TopicPartition(topic, 4),
                new TopicPartition(topic, 5),
                new TopicPartition(topic, 6),
                new TopicPartition(topic, 7)));

        consumer.seek(new TopicPartition(topic, 0), 0);
        consumer.seek(new TopicPartition(topic, 1), 0);
        consumer.seek(new TopicPartition(topic, 2), 0);
        consumer.seek(new TopicPartition(topic, 3), 0);
        consumer.seek(new TopicPartition(topic, 4), 0);
        consumer.seek(new TopicPartition(topic, 5), 0);
        consumer.seek(new TopicPartition(topic, 6), 0);
        consumer.seek(new TopicPartition(topic, 7), 0);

        consumer.poll(Duration.ofMillis(1000)).forEach(action);
    }

    private static Properties createKafkaProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.ap" +
                "ache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    private GenericRecord deserializeGenericRecord(final byte[] data) {
        try {
            final GenericDatumReader genericDatumReader = new GenericDatumReader(new Schema.Parser().parse(
                    new DefaultResourceLoader().getResource("classpath:nakadi.access.log.avsc").getInputStream()));
            return (GenericRecord) genericDatumReader.read(null,
                    DecoderFactory.get().binaryDecoder(data, null));
        } catch (final IOException io) {
            throw new RuntimeException();
        }
    }

}
