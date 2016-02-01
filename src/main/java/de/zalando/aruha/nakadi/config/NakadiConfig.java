package de.zalando.aruha.nakadi.config;

import com.fasterxml.jackson.databind.module.SimpleModule;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectDeserializer;
import com.fasterxml.jackson.datatype.jsonorg.JSONObjectSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.context.embedded.ServletRegistrationBean;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import org.springframework.core.env.Environment;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.servlets.MetricsServlet;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import com.ryantenney.metrics.spring.config.annotation.EnableMetrics;
import com.ryantenney.metrics.spring.config.annotation.MetricsConfigurerAdapter;

import de.zalando.aruha.nakadi.repository.kafka.KafkaFactory;
import de.zalando.aruha.nakadi.repository.kafka.KafkaLocationManager;
import de.zalando.aruha.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.problem.ProblemModule;

@Configuration
@EnableMetrics
@EnableScheduling
public class NakadiConfig {
    private static final Logger LOG = LoggerFactory.getLogger(NakadiConfig.class);

    public static final MetricRegistry METRIC_REGISTRY = new MetricRegistry();
    public static final HealthCheckRegistry HEALTH_CHECK_REGISTRY = new HealthCheckRegistry();

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
            public void configureReporters(final MetricRegistry metricRegistry) {
                // ConsoleReporter.forRegistry(metricRegistry).build().start(15, TimeUnit.SECONDS);
            }

            @Override
            public MetricRegistry getMetricRegistry() {
                return METRIC_REGISTRY;
            }
        };
    }

    @Bean
    @Primary
    public ObjectMapper jacksonObjectMapper() {
        ObjectMapper jsonMapper = new ObjectMapper().setPropertyNamingStrategy(
            PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        SimpleModule jsonObjectModule = new SimpleModule();
        jsonObjectModule.addSerializer(JSONObject.class, new JSONObjectSerializer());
        jsonObjectModule.addDeserializer(JSONObject.class, new JSONObjectDeserializer());

        jsonMapper.registerModule(jsonObjectModule);
        jsonMapper.registerModule(new Jdk8Module());
        jsonMapper.registerModule(new ProblemModule());

        return jsonMapper;
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

}
