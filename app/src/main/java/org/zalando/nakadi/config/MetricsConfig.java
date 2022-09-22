package org.zalando.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.dropwizard.DropwizardExports;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.metrics.NakadiMetricsServlet;

import java.lang.management.ManagementFactory;

@Configuration
@EnableMetrics
public class MetricsConfig {
    @Bean
    public ServletRegistrationBean servletRegistrationBean(final MetricRegistry metricRegistry) {
        return new ServletRegistrationBean(new NakadiMetricsServlet(metricRegistry), "/metrics/*");
    }

    class SubscriptionMetricsServlet extends MetricsServlet {
        SubscriptionMetricsServlet(final MetricRegistry metricRegistry) {
            super(metricRegistry);
        }
    }

    class StreamMetricsServlet extends MetricsServlet {
        StreamMetricsServlet(final MetricRegistry metricRegistry) {
            super(metricRegistry);
        }
    }

    @Bean
    public ServletRegistrationBean subscriptionsServletRegistrationBean(
            @Qualifier("perPathMetricRegistry") final MetricRegistry metricRegistry) {
        return new ServletRegistrationBean(new SubscriptionMetricsServlet(metricRegistry), "/request-metrics/*");
    }

    @Bean
    public ServletRegistrationBean streamMetricsServletRegistrationBean(
            @Qualifier("streamMetricsRegistry") final MetricRegistry metricRegistry) {
        return new ServletRegistrationBean(new StreamMetricsServlet(metricRegistry), "/stream-metrics/*");
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
    @Qualifier("perPathMetricRegistry")
    public MetricRegistry perPathMetricRegistry() {
        final MetricRegistry metricRegistry = new MetricRegistry();

        return metricRegistry;
    }

    @Bean
    @Qualifier("streamMetricsRegistry")
    public MetricRegistry streamMetricRegistry() {
        final MetricRegistry metricRegistry = new MetricRegistry();

        return metricRegistry;
    }

    @Bean
    public ServletRegistrationBean prometheusRegistrationBean(MetricRegistry metricRegistry,
        @Qualifier("perPathMetricRegistry") final MetricRegistry perPathMetricRegistry,
        @Qualifier("streamMetricsRegistry") final MetricRegistry streamMetricsRegistry) {
        CollectorRegistry collectorRegistry = new CollectorRegistry();
        collectorRegistry.register(new DropwizardExports(metricRegistry));
        collectorRegistry.register(new DropwizardExports(perPathMetricRegistry));
        collectorRegistry.register(new DropwizardExports(streamMetricsRegistry));
        return new ServletRegistrationBean(new io.prometheus.client.exporter.MetricsServlet(collectorRegistry), "/prometheus/*");
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
