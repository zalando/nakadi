package de.zalando.aruha.nakadi.repository.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import java.util.List;
import java.util.Properties;

class KafkaFactory implements KafkaPropertiesListener {
    private final KafkaLocationManager kafkaLocationManager;
    private volatile KafkaProducer<String, String> kafkaProducer;

    public KafkaFactory(final KafkaLocationManager kafkaLocationManager) {
        this.kafkaLocationManager = kafkaLocationManager;
        kafkaProducer = new KafkaProducer<>(kafkaLocationManager.getKafkaProperties());
        kafkaLocationManager.registerPropertiesListener(this);
    }

    public Producer<String, String> createProducer() {
        return kafkaProducer;
    }

    public Consumer<String, String> getConsumer() {
        return new KafkaConsumer<>(kafkaLocationManager.getKafkaProperties());
    }

    public NakadiKafkaConsumer createNakadiConsumer(final String topic, final List<KafkaCursor> kafkaCursors,
                                                    final long pollTimeout) {
        return new NakadiKafkaConsumer(getConsumer(), topic, kafkaCursors, pollTimeout);
    }

    @Override
    public void updateProperties(final Properties kafkaProperties) {
        kafkaProducer = new KafkaProducer<>(kafkaProperties);
    }
}
