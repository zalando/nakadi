package de.zalando.aruha.nakadi.webservice.utils;

import de.zalando.aruha.nakadi.domain.Cursor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class KafkaHelper {

    private final String kafkaUrl;

    public KafkaHelper(final String kafkaUrl) {
        this.kafkaUrl = kafkaUrl;
    }

    public KafkaConsumer<String, String> createConsumer() {
        return new KafkaConsumer<>(createKafkaProperties());
    }

    public KafkaProducer<String, String> createProducer() {
        return new KafkaProducer<>(createKafkaProperties());
    }

    private Properties createKafkaProperties() {
        final Properties props = new Properties();
        props.put("bootstrap.servers", kafkaUrl);
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        return props;
    }

    public void writeMessageToPartition(final String partition, final String topic, final String message)
            throws ExecutionException, InterruptedException {
        final String messageToSend = String.format("\"%s\"", message);
        final ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, Integer.parseInt(partition),
                "someKey", messageToSend);
        createProducer().send(producerRecord).get();
    }

    public void writeMultipleMessageToPartition(final String partition, final String topic, final String message,
                                                final int times)
            throws ExecutionException, InterruptedException {
        for (int i = 0; i < times; i++) {
            writeMessageToPartition(partition, topic, message);
        }
    }

    public List<Cursor> getNextOffsets(final String topic) {

        final KafkaConsumer<String, String> consumer = createConsumer();
        final List<TopicPartition> partitions = consumer
                .partitionsFor(topic)
                .stream()
                .map(pInfo -> new TopicPartition(topic, pInfo.partition()))
                .collect(Collectors.toList());

        consumer.assign(partitions);
        consumer.seekToEnd(partitions.toArray(new TopicPartition[partitions.size()]));

        return partitions
                .stream()
                .map(partition -> new Cursor(Integer.toString(partition.partition()),
                        Long.toString(consumer.position(partition))))
                .collect(Collectors.toList());
    }
}