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
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.repository.db.SubscriptionDbRepository;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import de.zalando.aruha.nakadi.service.ClosedConnectionsCrutch;
import de.zalando.aruha.nakadi.service.CursorsCommitService;
import de.zalando.aruha.nakadi.service.EventPublisher;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionKafkaClientFactory;
import de.zalando.aruha.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.lang.management.ManagementFactory;

@Configuration
@EnableMetrics
@EnableScheduling
public class NakadiConfig {

    @Autowired
    private JsonConfig jsonConfig;

    @Autowired
    private TopicRepository topicRepository;

    @Autowired
    private SubscriptionDbRepository subscriptionRepository;

    @Autowired
    private ZooKeeperHolder zooKeeperHolder;

    @Autowired
    private EventTypeCache eventTypeCache;

    @Autowired
    private EventTypeRepository eventTypeRepository;

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
    }

    @Bean
    public ServletRegistrationBean servletRegistrationBean(final MetricRegistry metricRegistry) {
        return new ServletRegistrationBean(new MetricsServlet(metricRegistry), "/metrics/*");
    }

    @Bean
    public MetricsConfigurerAdapter metricsConfigurerAdapter(final MetricRegistry metricRegistry) {
        return new MetricsConfigurerAdapter() {
            @Override
            public MetricRegistry getMetricRegistry() {
                return metricRegistry;
            }
        };
    }

    @Bean
    public EventStreamController eventStreamController(final ClosedConnectionsCrutch closedConnectionsCrutch,
                                                       final MetricRegistry metricRegistry)
    {
        return new EventStreamController(eventTypeRepository, topicRepository, jsonConfig.jacksonObjectMapper(),
                eventStreamFactory(), metricRegistry, closedConnectionsCrutch);
    }

    @Bean
    public VersionController versionController() {
        return new VersionController(jsonConfig.jacksonObjectMapper());
    }

    @Bean
    public ZooKeeperLockFactory zooKeeperLockFactory() {
        return new ZooKeeperLockFactory(zooKeeperHolder);
    }

    @Bean
    public ZkSubscriptionClientFactory zkSubscriptionClientFactory() {
        return new ZkSubscriptionClientFactory(zooKeeperHolder);
    }

    @Bean
    public SubscriptionKafkaClientFactory subscriptionKafkaClientFactory() {
        return new SubscriptionKafkaClientFactory(topicRepository, eventTypeRepository);
    }

    @Bean
    public CursorsCommitService cursorsCommitService() {
        return new CursorsCommitService(zooKeeperHolder, topicRepository, subscriptionRepository, eventTypeRepository,
                zooKeeperLockFactory(), zkSubscriptionClientFactory(), subscriptionKafkaClientFactory());
    }

    @Bean
    public EventStreamFactory eventStreamFactory() {
        return new EventStreamFactory();
    }

    @Bean
    public Enrichment enrichment() {
        return new Enrichment(new EnrichmentsRegistry());
    }

    @Bean
    public PartitionResolver partitionResolver() {
        return new PartitionResolver(topicRepository);
    }

    @Bean
    public PartitionsController partitionsController() {
        return new PartitionsController(eventTypeRepository, topicRepository);
    }

    @Bean
    public EventPublisher eventPublisher() {
        return new EventPublisher(topicRepository, eventTypeCache, partitionResolver(), enrichment());
    }

    @Bean
    public EventPublishingController eventPublishingController(final MetricRegistry metricRegistry) {
        return new EventPublishingController(eventPublisher(), eventTypeMetricRegistry(metricRegistry));
    }

    @Bean
    public EventTypeMetricRegistry eventTypeMetricRegistry(final MetricRegistry metricRegistry) {
        return new EventTypeMetricRegistry(metricRegistry);
    }

    @Bean
    public MetricRegistry metricRegistry() {
        final MetricRegistry metricRegistry = new MetricRegistry();

        metricRegistry.register("jvm.gc", new GarbageCollectorMetricSet());
        metricRegistry.register("jvm.buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
        metricRegistry.register("jvm.memory", new MemoryUsageGaugeSet());
        metricRegistry.register("jvm.threads", new ThreadStatesGaugeSet());

        return metricRegistry;
    }

}
