package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.db.EventTypeCachedRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import de.zalando.aruha.nakadi.validation.EventBodyMustRespectSchema;
import de.zalando.aruha.nakadi.validation.ValidationStrategy;
import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
public class RepositoriesConfig {

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private Environment environment;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ZookeeperConfig zookeeperConfig;

    @Bean
    public EventTypeCache eventTypeCache() throws Exception {
        final String connectString = zookeeperConfig.zooKeeperHolder().get().getZookeeperClient().getCurrentConnectionString();
        final RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
        final CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);

        client.start();

        EventBodyMustRespectSchema strategy = new EventBodyMustRespectSchema();
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, strategy);

        return new EventTypeCache(dbRepo(), client);
    }

    @Bean
    public EventTypeRepository eventTypeRepository() throws Exception {
        return new EventTypeCachedRepository(dbRepo(), eventTypeCache());
    }

    private EventTypeRepository dbRepo() {
        return new EventTypeDbRepository(jdbcTemplate, jsonConfig.jacksonObjectMapper());
    }
}
