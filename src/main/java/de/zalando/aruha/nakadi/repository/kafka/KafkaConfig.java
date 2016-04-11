package de.zalando.aruha.nakadi.repository.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;

@Configuration
@Import(ZookeeperConfig.class)
@Profile("!test")
public class KafkaConfig {

    @Autowired
    private ZookeeperConfig zookeeperConfig;

    @Bean
    public KafkaLocationManager getKafkaLocationManager() {
        return new KafkaLocationManager();
    }

    @Bean
    public KafkaRepositorySettings kafkaRepositorySettings() {
        return new KafkaRepositorySettings();
    }

    @Bean
    public KafkaFactory kafkaFactory() {
        return new KafkaFactory(getKafkaLocationManager());
    }

    @Bean
    public KafkaPartitionsCalculator kafkaPartitionsCalculator(
            @Value("${nakadi.kafka.instanceType}") final String instainceType,
            ObjectMapper objectMapper) throws IOException {
        return KafkaPartitionsCalculator.load(objectMapper, instainceType);
    }

    @Bean
    public KafkaTopicRepository kafkaTopicRepository(KafkaPartitionsCalculator partitionsCalculator) {
        return new KafkaTopicRepository(zookeeperConfig.zooKeeperHolder(), kafkaFactory(), kafkaRepositorySettings(), partitionsCalculator);
    }


}
