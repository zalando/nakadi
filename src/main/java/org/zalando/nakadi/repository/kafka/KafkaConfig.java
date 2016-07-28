package org.zalando.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.zalando.nakadi.repository.zookeeper.ZookeeperConfig;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

import java.io.IOException;

@Configuration
@Import(ZookeeperConfig.class)
@Profile("!test")
public class KafkaConfig {

    @Bean
    public KafkaPartitionsCalculator kafkaPartitionsCalculator(
            @Value("${nakadi.kafka.instanceType}") final String instanceType,
            final ObjectMapper objectMapper) throws IOException
    {
        return KafkaPartitionsCalculator.load(objectMapper, instanceType);
    }

}
