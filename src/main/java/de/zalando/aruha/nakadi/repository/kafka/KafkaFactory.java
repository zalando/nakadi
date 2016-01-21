package de.zalando.aruha.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager.Broker;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaFactory {
    private final KafkaLocationManager kafkaLocationManager;
    private final KafkaProducer<String, String> kafkaProducer;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager) {
        this.kafkaLocationManager = kafkaLocationManager;
        kafkaProducer = new KafkaProducer<>(kafkaLocationManager.getKafkaProperties());
    }

    public Producer<String, String> createProducer() {
        return kafkaProducer;
    }

    public Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(kafkaLocationManager.getKafkaProperties());
    }

    public SimpleConsumer getSimpleConsumer() {
        for (Broker kafkaBroker : kafkaLocationManager.getKafkaBrokers()) {
            return new SimpleConsumer(kafkaBroker.host, kafkaBroker.port, 1000, 64 * 1024, "leaderlookup");
        }
        return null;
    }
}
