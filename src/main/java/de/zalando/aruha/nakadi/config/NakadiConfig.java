package de.zalando.aruha.nakadi.config;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.web.WebApplicationInitializer;

@Configuration
@EnableMetrics
public class NakadiConfig {
	private static final Logger LOG = LoggerFactory.getLogger(NakadiConfig.class);

	public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
	public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

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
			public void configureReporters(final MetricRegistry metricRegistry) {
				//ConsoleReporter.forRegistry(metricRegistry).build().start(15, TimeUnit.SECONDS);
			}

			@Override
			public MetricRegistry getMetricRegistry() {
				return METRIC_REGISTRY;
			}
		};
	}
}