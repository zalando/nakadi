package de.zalando.aruha.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;

import org.springframework.stereotype.Component;

import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager.Broker;
import kafka.javaapi.consumer.SimpleConsumer;

@Component
class KafkaFactory {
    @Autowired
    private KafkaLocationManager kafkaLocationManager;

    @Autowired
    private Producer<String, String> producer;

    public KafkaFactory() { }

    @Bean
    public Producer<String, String> createProducer() {
        return new KafkaProducer<>(kafkaLocationManager.getKafkaProperties());
    }

    public Producer<String, String> getProducer() {
        return producer;
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
