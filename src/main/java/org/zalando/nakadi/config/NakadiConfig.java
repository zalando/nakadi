package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperHolder;
import org.zalando.nakadi.repository.zookeeper.ZooKeeperLockFactory;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClientFactory;

@Configuration
@EnableScheduling
public class NakadiConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(NakadiConfig.class);

    @Bean
    public TaskExecutor taskExecutor() {
        return new SimpleAsyncTaskExecutor();
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
    public SystemProperties systemProperties(final ApplicationContext context) {
        return name -> context.getEnvironment().getProperty(name);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public ApplicationService applicationService(@Value("${nakadi.auth.plugin.factory}") final String factoryName,
                                                 final SystemProperties systemProperties,
                                                 final DefaultResourceLoader loader) {
        try {
            LOGGER.info("Initialize application service factory: " + factoryName);
            final Class<ApplicationServiceFactory> factoryClass =
                    (Class<ApplicationServiceFactory>) loader.getClassLoader().loadClass(factoryName);
            final ApplicationServiceFactory factory = factoryClass.newInstance();
            return factory.init(systemProperties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BeanCreationException("Can't create ApplicationService " + factoryName, e);
        }
    }

}
