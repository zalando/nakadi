package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.zalando.nakadi.repository.kafka.KafkaConfig;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZookeeperConfig;
import org.zalando.nakadi.service.FeatureToggleService;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.domain.FeatureWrapper;
import org.zalando.nakadi.service.FeatureToggleServiceZk;

import java.util.Set;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
@EnableConfigurationProperties(FeaturesConfig.class)
public class RepositoriesConfig {

    private static final Logger LOG = LoggerFactory.getLogger(RepositoriesConfig.class);

    @Profile({"acceptanceTest", "local", "review"})
    @Bean
    public FeatureToggleService featureToggleServiceLocal(final ZooKeeperHolder zooKeeperHolder,
                                                          final FeaturesConfig featuresConfig) {
        final FeatureToggleService featureToggleService = new FeatureToggleServiceZk(zooKeeperHolder);
        if (featuresConfig.containsDefaults()) {
            final Set<String> features = featuresConfig.getFeaturesWithDefaultState();
            for (final String featureStr : features) {
                final boolean defaultState = featuresConfig.getDefaultState(featureStr);
                LOG.info("Setting feature {} to {}", featureStr, defaultState);
                final FeatureWrapper featureWrapper = new FeatureWrapper(Feature.valueOf(featureStr), defaultState);
                featureToggleService.setFeature(featureWrapper);
            }
        }
        return featureToggleService;
    }

    @Profile({"default", "dev", "qa", "uat", "prod"})
    @Bean
    public FeatureToggleService featureToggleService(final ZooKeeperHolder zooKeeperHolder) {
        return new FeatureToggleServiceZk(zooKeeperHolder);
    }

}
