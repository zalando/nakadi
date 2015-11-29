package de.zalando.aruha.nakadi.repository.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;

import org.springframework.beans.factory.annotation.Autowired;

import org.springframework.stereotype.Component;

import de.zalando.aruha.nakadi.config.KafkaConfiguration;

@Component
public class KafkaClientFactory {
    @Autowired
    private KafkaConfiguration configuration;

    public Producer createProducer() {

        final Properties props = new Properties();
        props.put("metadata.broker.list", configuration.getZookeeperAddress());

        final Producer<KafkaDefaults.KeySerializer, KafkaDefaults.ValueSerializer> p = new KafkaProducer<>(props);

        return p;
    }
}
