package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.CachingEventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import de.zalando.aruha.nakadi.validation.EventBodyMustRespectSchema;
import de.zalando.aruha.nakadi.validation.ValidationStrategy;
import org.apache.curator.framework.CuratorFramework;
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
        final CuratorFramework client = zookeeperConfig.zooKeeperHolder().get();
        final EventTypeCache cache = new EventTypeCache(dbRepo(), client);

        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());

        return new EventTypeCache(dbRepo(), client);
    }

    @Bean
    public EventTypeRepository eventTypeRepository() throws Exception {
        return new CachingEventTypeRepository(dbRepo(), eventTypeCache());
    }

    private EventTypeRepository dbRepo() {
        return new EventTypeDbRepository(jdbcTemplate, jsonConfig.jacksonObjectMapper());
    }
}
