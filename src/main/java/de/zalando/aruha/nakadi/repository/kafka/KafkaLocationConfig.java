package de.zalando.aruha.nakadi.repository.kafka;

import com.google.common.base.Strings;
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
            return kafkaAddress;
        }
        else {
            return linkedContainerKafkaAddress + ":" + linkedContainerKafkaPort;
        }
    }

    @Bean(name = "zookeeperBrokers")
    public String zookeeperBrokers() {
        if (Strings.isNullOrEmpty(linkedContainerZookeeperAddress)) {
            return zookeeperAddress;
        }
        else {
            return linkedContainerZookeeperAddress + ":" + linkedContainerZookeeperPort;
        }
    }

}
