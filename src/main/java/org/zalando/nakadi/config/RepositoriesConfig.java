package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.service.FeatureToggleServiceZk;
import org.zalando.nakadi.validation.EventBodyMustRespectSchema;
import org.zalando.nakadi.validation.EventMetadataValidationStrategy;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.ValidationStrategy;

import java.util.Set;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
public class RepositoriesConfig {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesConfig.class);

    @Bean
    public FeatureToggleService featureToggleService(
            @Value("${nakadi.featureToggle.default}") final boolean forceDefault,
            final ZooKeeperHolder zooKeeperHolder, final FeaturesConfig featuresConfig) {
        final FeatureToggleService featureToggleService = new FeatureToggleServiceZk(zooKeeperHolder);
        if (forceDefault) {
            final Set<String> features = featuresConfig.getDefaultFeatures().keySet();
            for (final String feature: features) {
                LOG.info("Setting feature {} to {}", feature, featuresConfig.getDefault(feature));
                featureToggleService.setFeature(
                        new FeatureToggleService.FeatureWrapper(FeatureToggleService.Feature.valueOf(feature),
                                featuresConfig.getDefault(feature)));
            }
        }
        return featureToggleService;
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
