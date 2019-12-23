package org.zalando.nakadi.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.BeanCreationException;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.DefaultResourceLoader;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.plugin.api.ApplicationServiceFactory;
import org.zalando.nakadi.plugin.api.SystemProperties;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.AuthorizationServiceFactory;

@Configuration
public class PluginsConfig {

    private static final Logger LOGGER = LoggerFactory.getLogger(PluginsConfig.class);

    @Bean
    public SystemProperties systemProperties(final ApplicationContext context) {
        return name -> context.getEnvironment().getProperty(name);
    }

    @Bean
    @SuppressWarnings("unchecked")
    public ApplicationService applicationService(@Value("${nakadi.plugins.auth.factory}") final String factoryName,
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

    @Bean
    public AuthorizationService authorizationService(@Value("${nakadi.plugins.authz.factory}") final String factoryName,
                                                     final SystemProperties systemProperties,
                                                     final DefaultResourceLoader loader) {
        try {
            LOGGER.info("Initialize per-resource authorization service factory: " + factoryName);
            final Class<AuthorizationServiceFactory> factoryClass =
                    (Class<AuthorizationServiceFactory>) loader.getClassLoader().loadClass(factoryName);
            final AuthorizationServiceFactory factory = factoryClass.newInstance();
            return factory.init(systemProperties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BeanCreationException("Can't create AuthorizationService " + factoryName, e);
        }
    }

    @Bean
    public TerminationService authorizationService(@Value("${nakadi.plugins.termination.factory}") final String factoryName,
                                                   final SystemProperties systemProperties,
                                                   final DefaultResourceLoader loader) {
        try {
            LOGGER.info("Initialize per-resource termination service factory: " + factoryName);
            final Class<TerminationServiceFactory> factoryClass =
                    (Class<TerminationServiceFactory>) loader.getClassLoader().loadClass(factoryName);
            final TerminationServiceFactory factory = factoryClass.newInstance();
            return factory.init(systemProperties);
        } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
            throw new BeanCreationException("Can't create TerminationService " + factoryName, e);
        }
    }
}
