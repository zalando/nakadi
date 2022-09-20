package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.repository.zookeeper.ZookeeperConfig;

import java.io.IOException;

@Configuration
@Import(ZookeeperConfig.class)
@Profile("!test")
public class KafkaConfig {

    @Bean
    public PartitionsCalculator createPartitionsCalculator(final NakadiSettings nakadiSettings) {
        return PartitionsCalculator.load(nakadiSettings);
    }
}
