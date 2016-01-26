package de.zalando.aruha.nakadi.webservice.utils;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.Properties;

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
}
