package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.controller.EventPublishingController;
import de.zalando.aruha.nakadi.repository.InMemoryEventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryTopicRepository;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

@Configuration
public class WebConfig extends WebMvcConfigurationSupport {

    @Value("${nakadi.stream.timeoutMs}")
    private long nakadiStreamTimeout;

    @Override
    public void configureAsyncSupport(final AsyncSupportConfigurer configurer) {
        configurer.setDefaultTimeout(nakadiStreamTimeout);
        configurer.registerCallableInterceptors(timeoutInterceptor());
    }

    @Bean
    public TimeoutCallableProcessingInterceptor timeoutInterceptor() {
        return new TimeoutCallableProcessingInterceptor();
    }

    @Bean
    public EventPublishingController eventPublishingController() {
        return new EventPublishingController(new InMemoryTopicRepository(), new InMemoryEventTypeRepository());
    }
}
