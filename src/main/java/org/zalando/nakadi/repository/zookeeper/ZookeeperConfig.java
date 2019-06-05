package org.zalando.nakadi.repository.zookeeper;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;

@Configuration
@Profile("!test")
public class ZookeeperConfig {

    @Bean
    public ZooKeeperHolder zooKeeperHolder(final Environment environment) throws Exception {
        return new ZooKeeperHolder(
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", ""),
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0")),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.sessionTimeoutMs")),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.connectionTimeoutMs"))
        );
    }
}
