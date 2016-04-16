package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import org.springframework.beans.factory.annotation.Autowired;
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
        return new KafkaLocationManager(zookeeperConfig.zooKeeperHolder());
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
    public KafkaTopicRepository kafkaTopicRepository() {
        return new KafkaTopicRepository(zookeeperConfig.zooKeeperHolder(), kafkaFactory(), kafkaRepositorySettings());
    }


}
