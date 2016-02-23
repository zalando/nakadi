package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import de.zalando.aruha.nakadi.controller.EventPublishingController;
import de.zalando.aruha.nakadi.controller.EventStreamController;
import de.zalando.aruha.nakadi.controller.PartitionsController;
import de.zalando.aruha.nakadi.partitioning.PartitionsCache;
import de.zalando.aruha.nakadi.repository.kafka.KafkaConfig;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;

@Configuration
@EnableMetrics
@EnableScheduling
public class NakadiConfig {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private KafkaConfig kafkaConfig;

    @Autowired
    private RepositoriesConfig repositoriesConfig;

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
    public EventStreamController eventStreamController() {
        return new EventStreamController(kafkaConfig.kafkaTopicRepository(), jsonConfig.jacksonObjectMapper(),
                eventStreamFactory());
    }

    @Bean
    public EventStreamFactory eventStreamFactory() {
        return new EventStreamFactory();
    }

    @Bean
    public PartitionsController partitionsController() {
        return new PartitionsController(kafkaConfig.kafkaTopicRepository());
    }

    @Bean
    public EventPublishingController eventPublishingController() {
        return new EventPublishingController(kafkaConfig.kafkaTopicRepository(),
                repositoriesConfig.eventTypeRepository(),
                numberOfPartionsCache());
    }

    @Bean
    public PartitionsCache numberOfPartionsCache() {
        return new PartitionsCache(kafkaConfig.kafkaTopicRepository());
    }

}
