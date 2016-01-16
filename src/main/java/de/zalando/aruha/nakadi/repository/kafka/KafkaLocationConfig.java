package de.zalando.aruha.nakadi.repository.kafka;

import org.springframework.beans.factory.annotation.Value;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
@PropertySource("${nakadi.config}")
public class KafkaLocationConfig {

  @Value("${nakadi.kafka.broker}")
  private String kafkaAddress;

  @Value("${nakadi.zookeeper.brokers}")
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
