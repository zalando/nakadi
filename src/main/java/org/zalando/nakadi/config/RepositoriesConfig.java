package org.zalando.nakadi.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.zalando.nakadi.annotations.DB;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.db.TimelineDbRepository;
import org.zalando.nakadi.repository.kafka.KafkaConfig;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperConfig;
import org.zalando.nakadi.service.timeline.TimelineSync;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.FeatureToggleServiceDefault;
import org.zalando.nakadi.util.FeatureToggleServiceZk;
import org.zalando.nakadi.validation.EventBodyMustRespectSchema;
import org.zalando.nakadi.validation.EventMetadataValidationStrategy;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.ValidationStrategy;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
public class RepositoriesConfig {

    @Bean
    public FeatureToggleService featureToggleService(
            @Value("${nakadi.featureToggle.default}") final boolean forceDefault,
            final ZooKeeperHolder zooKeeperHolder) {
        if (forceDefault) {
            return new FeatureToggleServiceDefault();
        } else {
            return new FeatureToggleServiceZk(zooKeeperHolder);
        }
    }

    @Bean
    public EventTypeCache eventTypeCache(final ZooKeeperHolder zooKeeperHolder,
                                         @DB final EventTypeRepository eventTypeRepository,
                                         @DB final TimelineDbRepository timelineRepository,
                                         final TimelineSync timelineSync) {
        ValidationStrategy.register(EventBodyMustRespectSchema.NAME, new EventBodyMustRespectSchema(
                new JsonSchemaEnrichment()
        ));
        ValidationStrategy.register(EventMetadataValidationStrategy.NAME, new EventMetadataValidationStrategy());

        try {
            return new EventTypeCache(eventTypeRepository, timelineRepository, zooKeeperHolder, timelineSync);
        } catch (final Exception e) {
            throw new IllegalStateException("failed to create event type cache", e);
        }
    }

}
