package org.zalando.nakadi.repository.kafka;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.assertj.core.util.Lists;
import org.zalando.nakadi.view.Cursor;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static org.zalando.nakadi.repository.kafka.KafkaCursor.toKafkaOffset;
import static org.zalando.nakadi.repository.kafka.KafkaCursor.toNakadiOffset;

public class KafkaTestHelper {

    public static final int CURSOR_OFFSET_LENGTH = 18;
    private final String kafkaUrl;

    public KafkaTestHelper(final String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }

    public KafkaConsumer<byte[], byte[]> createConsumer() {
        return new KafkaConsumer<>(createKafkaProperties());
    }

    public KafkaProducer<byte[], byte[]> createProducer() {
        return new KafkaProducer<>(createKafkaProperties());
    }

    protected static Properties createKafkaProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:29092");
        props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
        return props;
    }

    public void writeMessageToPartition(final String partition, final String topic, final String message)
            throws ExecutionException, InterruptedException {
        final String messageToSend = String.format("\"%s\"", message);
        final ProducerRecord<byte[], byte[]> producerRecord = new ProducerRecord<>(topic, Integer.parseInt(partition),
                "someKey".getBytes(StandardCharsets.UTF_8), messageToSend.getBytes(StandardCharsets.UTF_8));
        createProducer().send(producerRecord).get();
    }

    public void writeMultipleMessageToPartition(final String partition, final String topic, final String message,
                                                final int times)
            throws ExecutionException, InterruptedException {
        for (int i = 0; i < times; i++) {
            writeMessageToPartition(partition, topic, message);
        }
    }

    public List<Cursor> getOffsetsToReadFromLatest(final String topic) {
        return getNextOffsets(topic)
                .stream()
                .map(cursor -> {
                    if ("0".equals(cursor.getOffset())) {
                        return new Cursor(cursor.getPartition(), "001-0001--1");
                    } else {
                        final long lastEventOffset = toKafkaOffset(cursor.getOffset()) - 1;
                        final String offset = StringUtils.leftPad(toNakadiOffset(lastEventOffset),
                                CURSOR_OFFSET_LENGTH, '0');
                        return new Cursor(cursor.getPartition(), String.format("001-0001-%s", offset));
                    }
                })
                .collect(Collectors.toList());
    }

    public List<Cursor> getNextOffsets(final String topic) {

        final KafkaConsumer<byte[], byte[]> consumer = createConsumer();
        final List<TopicPartition> partitions = consumer
                .partitionsFor(topic)
                .stream()
                .map(pInfo -> new TopicPartition(topic, pInfo.partition()))
                .collect(Collectors.toList());

        consumer.assign(partitions);
        consumer.seekToEnd(partitions);

        return partitions
                .stream()
                .map(partition -> new Cursor(Integer.toString(partition.partition()),
                        Long.toString(consumer.position(partition))))
                .collect(Collectors.toList());
    }

    public void createTopic(final String topic) {
        try (AdminClient adminClient = AdminClient.create(createKafkaProperties())) {
            adminClient.createTopics(Lists.newArrayList(
                    new NewTopic(topic, Optional.of(1), Optional.of((short) 1))
            ));
        }
    }

    public static Long getTopicRetentionTime(final String topic)
            throws ExecutionException, InterruptedException {
        return Long.valueOf(getTopicProperty(topic, "retention.ms"));
    }

    public static String getTopicCleanupPolicy(final String topic)
            throws ExecutionException, InterruptedException {
        return getTopicProperty(topic, "cleanup.policy");
    }

    public static String getTopicProperty(final String topic, final String propertyName)
            throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(createKafkaProperties())) {
            final ConfigResource configResource = new ConfigResource(ConfigResource.Type.TOPIC, topic);
            final DescribeConfigsResult result = adminClient.describeConfigs(Lists.newArrayList(configResource));
            return result
                    .values()
                    .get(configResource)
                    .get()
                    .entries()
                    .stream()
                    .filter(entry -> entry.name().equals(propertyName))
                    .findFirst()
                    .get()
                    .value();
        }
    }
}
