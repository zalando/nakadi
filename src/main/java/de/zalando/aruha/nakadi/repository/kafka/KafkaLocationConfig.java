package de.zalando.aruha.nakadi.repository.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
public class KafkaLocationConfig {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationConfig.class);

    @Value("${KAFKA_BROKERS}")
    private String kafkaAddress;

    @Value("${ZK_BROKERS}")
    private String zookeeperAddress;

    @Bean(name = "kafkaBrokers")
    public String kafkaBrokers() {
        return kafkaAddress;
    }

    @Bean(name = "zookeeperBrokers")
    public String zookeeperBrokers() {
        return zookeeperAddress;
    }

}
