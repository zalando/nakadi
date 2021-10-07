package org.zalando.nakadi.webservice;

import com.google.common.collect.ImmutableList;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.service.publishing.AvroEventPublisher;
import org.zalando.nakadi.webservice.utils.NakadiTestUtils;

import java.io.IOException;
import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

public class AvroAT extends BaseAT {

    @Test
    public void test() throws IOException {
        final EventType eventType = NakadiTestUtils.createEventType();
        NakadiTestUtils.publishEvent(eventType.getName(), "{\"foo\":\"bar\"}");

        consumeAvro(consumerRecord -> {
            System.out.println(new String(consumerRecord.headers()
                    .lastHeader(NakadiRecord.HEADER_EVENT_TYPE).value()));
            System.out.println(new String(consumerRecord.value()));
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
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }
}
