package de.zalando.aruha.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("${nakadi.config}")
public class KafkaLocationConfig {

    @Value("${nakadi.zookeeper.brokers}")
    private String zookeeperAddress;

    @Value("${nakadi.zookeeper.kafkaNamespace}")
    private String zookeeperKafkaNamespace;

    @Value("${nakadi.exhibitor.address}")
    private String exhibitorAddress;

    @Value("${nakadi.exhibitor.port}")
    private Integer exhibitorPort;

    @Bean(name = "zookeeperBrokers")
    public String zookeeperBrokers() {
        return zookeeperAddress;
    }

    @Bean(name = "zookeeperKafkaNamespace")
    public String zookeeperKafkaNamespace() {
        return zookeeperKafkaNamespace;
    }

    @Bean(name = "exhibitorAddress")
    public String exhibitorAddress() {
        return exhibitorAddress;
    }

    @Bean(name = "exhibitorPort")
    public Integer exhibitorPort() {
        return exhibitorPort;
    }

    @Bean
    public KafkaLocationManager getKafkaLocationManager() {
        return new KafkaLocationManager();
    }
}
