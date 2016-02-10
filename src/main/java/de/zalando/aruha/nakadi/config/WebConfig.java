package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.FlowIdRequestFilter;
import de.zalando.aruha.nakadi.controller.EventPublishingController;
import de.zalando.aruha.nakadi.repository.InMemoryEventTypeRepository;
import de.zalando.aruha.nakadi.repository.kafka.KafkaRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.embedded.FilterRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.web.context.request.async.TimeoutCallableProcessingInterceptor;
import org.springframework.web.servlet.config.annotation.AsyncSupportConfigurer;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurationSupport;

@Configuration
public class WebConfig extends WebMvcConfigurationSupport {

    @Value("${nakadi.stream.timeoutMs}")
    private long nakadiStreamTimeout;

    @Autowired
    private KafkaRepository kafkaRepository;

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
        return new EventPublishingController(kafkaRepository, new InMemoryEventTypeRepository());
    }

    @Bean
    public FilterRegistrationBean flowIdRequestFilter() {
        final FilterRegistrationBean filterRegistrationBean = new FilterRegistrationBean();
        filterRegistrationBean.setFilter(new FlowIdRequestFilter());
        filterRegistrationBean.setOrder(Ordered.HIGHEST_PRECEDENCE);
        return filterRegistrationBean;
    }
}
