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
import org.zalando.nakadi.util.FeatureToggleServiceZk;
import org.zalando.nakadi.validation.EventBodyMustRespectSchema;
import org.zalando.nakadi.validation.EventMetadataValidationStrategy;
import org.zalando.nakadi.validation.JsonSchemaEnrichment;
import org.zalando.nakadi.validation.ValidationStrategy;

import static org.zalando.nakadi.util.FeatureToggleService.Feature;
import static org.zalando.nakadi.util.FeatureToggleService.FeatureWrapper;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
public class RepositoriesConfig {

    @Bean
    public FeatureToggleService featureToggleService(
            @Value("${nakadi.featureToggle.default}") final boolean forceDefault,
            final ZooKeeperHolder zooKeeperHolder) {
        FeatureToggleService featureToggleService = new FeatureToggleServiceZk(zooKeeperHolder);
        if (forceDefault) {
            featureToggleService.setFeature(new FeatureWrapper(Feature.CHECK_OWNING_APPLICATION, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.CHECK_PARTITIONS_KEYS, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.CONNECTION_CLOSE_CRUTCH, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.DISABLE_DB_WRITE_OPERATIONS, false));
            featureToggleService.setFeature(new FeatureWrapper(Feature.DISABLE_EVENT_TYPE_CREATION, false));
            featureToggleService.setFeature(new FeatureWrapper(Feature.DISABLE_EVENT_TYPE_DELETION, false));
            featureToggleService.setFeature(new FeatureWrapper(Feature.DISABLE_SUBSCRIPTION_CREATION, false));
            featureToggleService.setFeature(new FeatureWrapper(Feature.HIGH_LEVEL_API, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.KPI_COLLECTION, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.LIMIT_CONSUMERS_NUMBER, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.REMOTE_TOKENINFO, true));
            featureToggleService.setFeature(new FeatureWrapper(Feature.SEND_BATCH_VIA_OUTPUT_STREAM, true));
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
