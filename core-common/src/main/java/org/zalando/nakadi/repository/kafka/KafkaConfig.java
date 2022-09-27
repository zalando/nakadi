package org.zalando.nakadi.repository.kafka;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.repository.zookeeper.ZookeeperConfig;

@Configuration
@Import(ZookeeperConfig.class)
@Profile("!test")
public class KafkaConfig {

    @Bean
    public PartitionsCalculator createPartitionsCalculator(final NakadiSettings nakadiSettings) {
        return PartitionsCalculator.load(nakadiSettings);
    }
}
