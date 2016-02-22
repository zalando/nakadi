package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepositorySettings;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@Profile("!test")
public class RepositoriesConfig {

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private Environment environment;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Bean
    public EventTypeRepository eventTypeRepository() {
        return new EventTypeDbRepository(jdbcTemplate, jsonConfig.jacksonObjectMapper());
    }

    @Bean
    public ZooKeeperHolder zooKeeperHolder() {
        return new ZooKeeperHolder(
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", ""),
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0"))
        );
    }

    @Bean
    public KafkaLocationManager getKafkaLocationManager() {
        return new KafkaLocationManager();
    }

    @Bean
    public KafkaFactory kafkaFactory() {
        return new KafkaFactory(getKafkaLocationManager());
    }

    @Bean
    public KafkaRepositorySettings kafkaRepositorySettings() {
        return new KafkaRepositorySettings();
    }

    @Bean
    public KafkaTopicRepository kafkaRepository() {
        return new KafkaTopicRepository(zooKeeperHolder(), kafkaFactory(), kafkaRepositorySettings());
    }

}
