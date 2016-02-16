package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectDeserializer;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import de.zalando.aruha.nakadi.controller.EventPublishingController;
import de.zalando.aruha.nakadi.controller.EventStreamController;
import de.zalando.aruha.nakadi.controller.PartitionsController;
import de.zalando.aruha.nakadi.repository.db.EventTypeDbRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager;
import de.zalando.aruha.nakadi.repository.kafka.KafkaTopicRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepositorySettings;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.json.JSONObject;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.zalando.problem.ProblemModule;

@Configuration
@EnableMetrics
@EnableScheduling
public class NakadiConfig {

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();

    @Autowired
    private Environment environment;
    
    @Autowired
    private EventTypeDbRepository eventTypeDbRepository;

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
        ObjectMapper objectMapper = new ObjectMapper().setPropertyNamingStrategy(
            PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        SimpleModule jsonObjectModule = new SimpleModule();
        jsonObjectModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        jsonObjectModule.addDeserializer(JSONObject.class, new JSONObjectDeserializer());

        objectMapper.registerModule(jsonObjectModule);
        objectMapper.registerModule(new Jdk8Module());
        objectMapper.registerModule(new ProblemModule());

        return objectMapper;
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

    @Bean
    public EventStreamController eventStreamController() {
        return new EventStreamController(kafkaRepository(), jacksonObjectMapper(), eventStreamFactory());
    }

    @Bean
    public EventStreamFactory eventStreamFactory() {
        return new EventStreamFactory();
    }

    @Bean
    public PartitionsController partitionsController() {
        return new PartitionsController(kafkaRepository());
    }

    @Bean
    public EventPublishingController eventPublishingController() {
        return new EventPublishingController(kafkaRepository(), eventTypeDbRepository);
    }

}
