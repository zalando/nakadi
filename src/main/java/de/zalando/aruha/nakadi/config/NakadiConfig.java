package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import de.zalando.aruha.nakadi.controller.EventPublishingController;
import de.zalando.aruha.nakadi.controller.EventStreamController;
import de.zalando.aruha.nakadi.controller.PartitionsController;
import de.zalando.aruha.nakadi.controller.VersionController;
import de.zalando.aruha.nakadi.enrichment.Enrichment;
import de.zalando.aruha.nakadi.enrichment.EnrichmentsRegistry;
import de.zalando.aruha.nakadi.metrics.EventTypeMetricRegistry;
import de.zalando.aruha.nakadi.partitioning.PartitionResolver;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.service.EventPublisher;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import java.lang.management.ManagementFactory;
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

    public static final MetricRegistry METRIC_REGISTRY = createMetricRegistry();

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private TopicRepository topicRepository;

    @Autowired
    private EventTypeCache eventTypeCache;

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
        return new EventStreamController(topicRepository, jsonConfig.jacksonObjectMapper(),
                eventStreamFactory(), METRIC_REGISTRY);
    }

    @Bean
    public VersionController versionController() {
        return new VersionController(jsonConfig.jacksonObjectMapper());
    }

    @Bean
    public EventStreamFactory eventStreamFactory() {
        return new EventStreamFactory();
    }

    @Bean
    public Enrichment enrichment() { return new Enrichment(new EnrichmentsRegistry()); }

    @Bean
    public PartitionResolver partitionResolver() {
        return new PartitionResolver(topicRepository);
    }

    @Bean
    public PartitionsController partitionsController() {
        return new PartitionsController(topicRepository);
    }

    @Bean
    public EventPublisher eventPublisher() {
        return new EventPublisher(topicRepository, eventTypeCache, partitionResolver(), enrichment());
    }

    @Bean
    public EventPublishingController eventPublishingController() {
        return new EventPublishingController(eventPublisher(), eventTypeMetricRegistry());
    }

    @Bean
    public EventTypeMetricRegistry eventTypeMetricRegistry() {
        return new EventTypeMetricRegistry(METRIC_REGISTRY);
    }

    private static MetricRegistry createMetricRegistry() {
        final MetricRegistry metricRegistry = new MetricRegistry();

        metricRegistry.register("jvm.gc", new GarbageCollectorMetricSet());
        metricRegistry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        metricRegistry.register("jvm.memory", new MemoryUsageGaugeSet());
        metricRegistry.register("jvm.threads", new ThreadStatesGaugeSet());

        return metricRegistry;
    }

}
