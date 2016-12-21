package org.zalando.nakadi.stream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.repository.kafka.KafkaLocationManager;

import java.util.Properties;

@Component
@Profile("!test")
public class KafkaStreamConfig {

    private final KafkaLocationManager kafkaLocationManager;
    private final String exhibitorBrokers;
    private final String kafkaNamespace;

    @Autowired
    public KafkaStreamConfig(final KafkaLocationManager kafkaLocationManager,
                             @Value("${nakadi.zookeeper.kafkaNamespace}") final String kafkaNamespace,
                             @Value("${nakadi.zookeeper.brokers}") final String exhibitorBrokers) {
        this.kafkaLocationManager = kafkaLocationManager;
        this.exhibitorBrokers = exhibitorBrokers;
        this.kafkaNamespace = kafkaNamespace;
    }

    public Properties getProperties(final int applicationId) {
        final Properties properties = kafkaLocationManager.getKafkaProperties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "nakadi-streams-" + applicationId);
        properties.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG, exhibitorBrokers + kafkaNamespace);
        properties.put(StreamsConfig.KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // FIXME
        properties.put(StreamsConfig.VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName()); // FIXME
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 10 * 1000);
        return properties;
    }
}
