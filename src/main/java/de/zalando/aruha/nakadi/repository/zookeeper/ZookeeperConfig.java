package de.zalando.aruha.nakadi.repository.zookeeper;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Environment;

@Configuration
public class ZookeeperConfig {
    @Autowired
    private Environment environment;

    @Bean
    public ZooKeeperHolder zooKeeperHolder() {
        return new ZooKeeperHolder(
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", ""),
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0"))
        );
    }

}
