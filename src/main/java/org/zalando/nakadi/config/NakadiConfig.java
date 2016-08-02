package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;
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
    public ZooKeeperLockFactory zooKeeperLockFactory(final ZooKeeperHolder zooKeeperHolder) {
        return new ZooKeeperLockFactory(zooKeeperHolder);
    }

    @Bean
    public ZkSubscriptionClientFactory zkSubscriptionClientFactory(final ZooKeeperHolder zooKeeperHolder) {
        return new ZkSubscriptionClientFactory(zooKeeperHolder);
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

    @Bean
    public SystemProperties systemProperties(final ApplicationContext context) {
        return name -> context.getEnvironment().getProperty(name);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public ApplicationService applicationService(@Value("${nakadi.auth.plugin.factory}") final String factoryName,
                                                 final SystemProperties systemProperties)
    {
        try {
            final Class<ApplicationServiceFactory> factoryClass = (Class<ApplicationServiceFactory>) ClassLoader.getSystemClassLoader()
                    .loadClass(factoryName);
            final ApplicationServiceFactory factory = factoryClass.newInstance();
            return factory.init(systemProperties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BeanCreationException("Can't create ApplicationService " + factoryName, e);
        }
    }

}
