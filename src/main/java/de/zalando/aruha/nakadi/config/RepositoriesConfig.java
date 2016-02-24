package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.repository.zookeeper.ZookeeperConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
@Profile("!test")
@Import({KafkaConfig.class, ZookeeperConfig.class})
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
}
