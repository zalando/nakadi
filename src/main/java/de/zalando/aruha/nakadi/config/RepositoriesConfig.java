package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.annotations.DB;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import de.zalando.aruha.nakadi.util.FeatureToggleService;
import de.zalando.aruha.nakadi.util.FeatureToggleServiceDefault;
import de.zalando.aruha.nakadi.util.FeatureToggleServiceZk;
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

    @Bean
    public FeatureToggleService featureToggleService(@Value("${nakadi.featureToggle.default}") final boolean forceDefault,
                                                     final ZooKeeperHolder zooKeeperHolder)
    {
        if (forceDefault) {
            return new FeatureToggleServiceDefault();
        } else {
            return new FeatureToggleServiceZk(zooKeeperHolder);
        }
    }

    @Bean
    public EventTypeCache eventTypeCache(final ZooKeeperHolder zooKeeperHolder,
                                         @DB final EventTypeRepository eventTypeRepository)
    {
        final CuratorFramework client = zooKeeperHolder.get();
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema());
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());

        try {
            return new EventTypeCache(eventTypeRepository, client);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to create event type cache");
        }
    }

}
