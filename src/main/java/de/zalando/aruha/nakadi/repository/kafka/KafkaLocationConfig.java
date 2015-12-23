package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

@Configuration
public class KafkaLocationConfig {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaLocationConfig.class);

    @Value("${nakadi.kafka.broker}")
    private String kafkaAddress;

    @Value("${nakadi.zookeeper.brokers}")
    private String zookeeperAddress;

    // in a case if we run nakadi in a docker container with a linked kafka docker
    // container - we should get kafka and zk adresses from env variables set up by docker
    @Value("#{environment['LOCAL_KAFKA_PORT_9092_TCP_ADDR']}")
    private String linkedContainerKafkaAddress;

    @Value("#{environment['LOCAL_KAFKA_PORT_9092_TCP_PORT']}")
    private String linkedContainerKafkaPort;

    @Value("#{environment['LOCAL_KAFKA_PORT_2181_TCP_ADDR']}")
    private String linkedContainerZookeeperAddress;

    @Value("#{environment['LOCAL_KAFKA_PORT_2181_TCP_PORT']}")
    private String linkedContainerZookeeperPort;

    @Bean(name = "kafkaBrokers")
    public String kafkaBrokers() {
        if (Strings.isNullOrEmpty(linkedContainerKafkaAddress)) {
            if (LOG.isInfoEnabled()) {
                LOG.info("No linked Kafka container configured. Starting with Kafka brokers: " + kafkaAddress);
            }
            return kafkaAddress;
        }
        else {
            final String connectionString = linkedContainerKafkaAddress + ":" + linkedContainerKafkaPort;
            if (LOG.isInfoEnabled()) {
                LOG.info("Linked Kafka containers are configured. Starting with Kafka brokers: " + connectionString);
            }
            return connectionString;
        }
    }

    @Bean(name = "zookeeperBrokers")
    public String zookeeperBrokers() {
        if (Strings.isNullOrEmpty(linkedContainerZookeeperAddress)) {
                if (LOG.isInfoEnabled()) {
                    LOG.info("No linked Zookeeper container configured. Starting with Zookeeper brokers: " + kafkaAddress);
                }
            return zookeeperAddress;
        }
        else {
            final String connectionString = linkedContainerZookeeperAddress + ":" + linkedContainerZookeeperPort;
            if (LOG.isInfoEnabled()) {
                LOG.info("Linked Zookeeper containers are configured. Starting with Zookeeper brokers: " + connectionString);
            }
            return connectionString;
        }
    }

}
