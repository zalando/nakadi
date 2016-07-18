package de.zalando.aruha.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class KafkaFactory {
    private final KafkaLocationManager kafkaLocationManager;
    private final KafkaProducer<String, String> kafkaProducer;

    @Autowired
    public KafkaFactory(final KafkaLocationManager kafkaLocationManager) {
        this.kafkaLocationManager = kafkaLocationManager;
        kafkaProducer = new KafkaProducer<>(kafkaLocationManager.getKafkaProducerProperties());
    }

    public Producer<String, String> getProducer() {
        return kafkaProducer;
    }

    public Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(kafkaLocationManager.getKafkaProperties());
    }

    public NakadiKafkaConsumer createNakadiConsumer(final String topic, final List<KafkaCursor> kafkaCursors,
                                                    final long pollTimeout) {
        return new NakadiKafkaConsumer(getConsumer(), topic, kafkaCursors, pollTimeout);
    }

}
