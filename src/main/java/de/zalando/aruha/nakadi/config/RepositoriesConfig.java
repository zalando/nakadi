package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.CachingEventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import de.zalando.aruha.nakadi.util.FeatureToggleServiceDefault;
import de.zalando.aruha.nakadi.util.FeatureToggleServiceZk;
import de.zalando.aruha.nakadi.util.UUIDGenerator;
import de.zalando.aruha.nakadi.validation.EventBodyMustRespectSchema;
import de.zalando.aruha.nakadi.validation.EventMetadataValidationStrategy;
import de.zalando.aruha.nakadi.validation.ValidationStrategy;
import org.apache.curator.framework.CuratorFramework;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
public class RepositoriesConfig {

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private ZookeeperConfig zookeeperConfig;

    @Bean
    public FeatureToggleService featureToggleService(@Value("${nakadi.featureToggle.default}") final boolean forceDefault) {
        if (forceDefault) {
            return new FeatureToggleServiceDefault();
        } else {
            return new FeatureToggleServiceZk(zookeeperConfig.zooKeeperHolder());
        }
    }

    @Bean
    public UUIDGenerator uuidGenerator() {
        return new UUIDGenerator();
    }

    @Bean
    public EventTypeCache eventTypeCache() {
        final CuratorFramework client = zookeeperConfig.zooKeeperHolder().get();
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());

        try {
            return new EventTypeCache(eventTypeDBRepository(), client);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to create event type cache");
        }
    }

    @Bean
    public EventTypeRepository eventTypeRepository() {
        return new CachingEventTypeRepository(eventTypeDBRepository(), eventTypeCache());
    }

    @Bean
    public SubscriptionDbRepository subscriptionRepository() {
        return new SubscriptionDbRepository(jdbcTemplate, jsonConfig.jacksonObjectMapper(), uuidGenerator());
    }

    private EventTypeRepository eventTypeDBRepository() {
        return new EventTypeDbRepository(jdbcTemplate, jsonConfig.jacksonObjectMapper());
    }
}
