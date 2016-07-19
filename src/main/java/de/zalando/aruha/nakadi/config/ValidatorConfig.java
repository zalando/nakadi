package de.zalando.aruha.nakadi.config;

import de.zalando.aruha.nakadi.validation.EventTypeOptionsValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ValidatorConfig {

    @Bean
    public EventTypeOptionsValidator eventTypeOptionsValidator(final @Value("${nakadi.topic.min.retentionMs}") long minTopicRetentionMs,
                                                               final @Value("${nakadi.topic.max.retentionMs}") long maxTopicRetentionMs)
    {
        return new EventTypeOptionsValidator(minTopicRetentionMs, maxTopicRetentionMs);
    }
}
