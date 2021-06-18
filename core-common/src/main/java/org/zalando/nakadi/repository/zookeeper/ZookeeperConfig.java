package org.zalando.nakadi.repository.zookeeper;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.storage.ZookeeperConnection;

@Configuration
@Profile("!test")
public class ZookeeperConfig {

    @Bean
    public ZooKeeperHolder zooKeeperHolder(final Environment environment,
                                           final NakadiSettings nakadiSettings) throws Exception {
        return new ZooKeeperHolder(
                ZookeeperConnection.valueOf(environment.getProperty("nakadi.zookeeper.connection-string")),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.session-timeout-ms")),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.connection-timeout-ms")),
                nakadiSettings
        );
    }
}
