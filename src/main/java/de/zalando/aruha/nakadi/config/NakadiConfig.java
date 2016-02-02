package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.controller.EventStreamController;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepositorySettings;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import org.springframework.context.annotation.Profile;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;

import static com.fasterxml.jackson.databind.PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES;

@Configuration
@EnableMetrics
@EnableScheduling
public class NakadiConfig {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @Autowired
    private Environment environment;

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ServletRegistrationBean servletRegistrationBean() {
        return new ServletRegistrationBean(new MetricsServlet(METRIC_REGISTRY), "/metrics/*");
    }

    @Bean
    public MetricsConfigurerAdapter metricsConfigurerAdapter() {
        return new MetricsConfigurerAdapter() {
            @Override
            public MetricRegistry getMetricRegistry() {
                return METRIC_REGISTRY;
            }
        };
    }

    @Bean
    @Primary
    public ObjectMapper jacksonObjectMapper() {
        return new ObjectMapper().setPropertyNamingStrategy(CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
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
    @Profile("kafka")
    public KafkaLocationManager getKafkaLocationManager() {
        return new KafkaLocationManager();
    }

    @Bean
    @Profile("kafka")
    public KafkaFactory kafkaFactory() {
        return new KafkaFactory(getKafkaLocationManager());
    }

    @Bean
    @Profile("kafka")
    public KafkaRepositorySettings kafkaRepositorySettings() {
        return new KafkaRepositorySettings();
    }

    @Bean
    public TopicRepository topicRepository() {
        if (environment.acceptsProfiles("kafka")) {
            return new KafkaRepository(zooKeeperHolder(), kafkaFactory(), kafkaRepositorySettings());
        }
        else {
            throw new IllegalStateException("No TopicRepository implementation found for current spring profile");
        }
    }

    @Bean
    public EventStreamController eventStreamController() {
        return new EventStreamController(topicRepository(), jacksonObjectMapper());
    }

}
