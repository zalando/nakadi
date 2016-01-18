package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.ZkSubscriptionRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.service.EventStreamManager;
import de.zalando.aruha.nakadi.service.PartitionDistributor;
import de.zalando.aruha.nakadi.service.RegularPartitionDistributor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;

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
    public TopicRepository kafkaRepository() {
        return new KafkaRepository(zooKeeperHolder(), kafkaFactory(),
                Long.parseLong(environment.getProperty("nakadi.kafka.poll.timeoutMs")));
    }

    @Bean
    public EventStreamManager eventStreamManager() {
        return new EventStreamManager(subscriptionRepository(), partitionDistributor());
    }

    @Bean
    public SubscriptionRepository subscriptionRepository() {
        return new ZkSubscriptionRepository(zooKeeperHolder());
    }

    @Bean
    public PartitionDistributor partitionDistributor() {
        return new RegularPartitionDistributor(kafkaRepository(), subscriptionRepository());
    }

    @Bean
    public ZooKeeperHolder zooKeeperHolder() {
        System.out.println("creating zookeeper holder...");
        System.out.println("environment: " + environment);
        return new ZooKeeperHolder(
                environment.getProperty("nakadi.zookeeper.brokers"),
                environment.getProperty("nakadi.zookeeper.kafkaNamespace", ""),
                environment.getProperty("nakadi.zookeeper.exhibitor.brokers"),
                Integer.parseInt(environment.getProperty("nakadi.zookeeper.exhibitor.port", "0"))
        );
    }

    @Bean
    public KafkaLocationManager getKafkaLocationManager() {
        return new KafkaLocationManager(zooKeeperHolder());
    }

    @Bean
    public KafkaFactory kafkaFactory() {
        return new KafkaFactory(getKafkaLocationManager());
    }

}
