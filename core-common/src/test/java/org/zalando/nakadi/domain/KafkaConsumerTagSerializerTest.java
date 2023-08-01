package org.zalando.nakadi.domain;

import com.google.common.base.Charsets;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Assert;
import org.junit.Test;

import java.nio.charset.StandardCharsets;
import java.util.Map;

public class KafkaConsumerTagSerializerTest {

    private static final String SUB_ID = "16120729-4a57-4607-ad3a-d526a4590e75";

    @Test
    public void testConsumerTagSerializer() {
        final var consumerTags = Map.of(ConsumerTag.SUBSCRIPTION_ID, SUB_ID);
        final ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(
                "topic",
                "value".getBytes(StandardCharsets.UTF_8));
        KafkaConsumerTagSerializer.serialize(consumerTags, record);

        Assert.assertEquals(SUB_ID,
                new String(
                        record.headers().lastHeader(ConsumerTag.SUBSCRIPTION_ID.name()).value(),
                        Charsets.UTF_8));
    }

    @Test
    public void testConsumerTagDeserializer() {
        final ConsumerRecord<byte[], byte[]> record =
                new ConsumerRecord<>("topic", 1, 1L, "key".getBytes(), "value".getBytes());
        record.headers().add(ConsumerTag.SUBSCRIPTION_ID.name(), SUB_ID.getBytes(Charsets.UTF_8));

        final var consumerTags = KafkaConsumerTagSerializer.deserialize(record);
        Assert.assertEquals(consumerTags.get(ConsumerTag.SUBSCRIPTION_ID), SUB_ID);
    }

}
