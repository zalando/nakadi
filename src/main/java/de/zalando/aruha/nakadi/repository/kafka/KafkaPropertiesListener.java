package de.zalando.aruha.nakadi.repository.kafka;

import java.util.Properties;

public interface KafkaPropertiesListener {
    void updateProperties(final Properties kafkaProperties);
}
