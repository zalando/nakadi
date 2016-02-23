package de.zalando.aruha.nakadi.repository.kafka;

import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

@Configuration
@Import(ZooKeeperConfig.class)
public class KafkaConfig {

    @Autowired
    private ZooKeeperConfig zookeeperConfig;

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
    public KafkaTopicRepository kafkaTopicRepository() {
        return new KafkaTopicRepository(zookeeperConfig.zooKeeperHolder(), kafkaFactory(), kafkaRepositorySettings());
    }


}
