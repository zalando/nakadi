package de.zalando.aruha.nakadi.config;

import java.sql.NClob;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.servlet.ServletContainerInitializer;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.embedded.ServletContextInitializer;
import org.springframework.boot.context.embedded.ServletRegistrationBean;
import org.springframework.boot.context.web.ServletContextApplicationContextInitializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.WebApplicationInitializer;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.AdminServlet;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurationSupport;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

@Configuration
@EnableMetrics
public class NakadiConfig {
	private static final Logger LOG = LoggerFactory.getLogger(NakadiConfig.class);

	public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
	public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

	@Bean
	public ServletRegistrationBean servletRegistrationBean() {
		return new ServletRegistrationBean(new MetricsServlet(METRIC_REGISTRY), "/metrics/*");
	}

	@Bean
	public MetricsConfigurerAdapter metricsConfigurerAdapter() {
		final MetricsConfigurerAdapter mca = new MetricsConfigurerAdapter() {
			@Override
			public void configureReporters(final MetricRegistry metricRegistry) {
				//ConsoleReporter.forRegistry(metricRegistry).build().start(15, TimeUnit.SECONDS);
			}
		};
		return mca ;
	}
}