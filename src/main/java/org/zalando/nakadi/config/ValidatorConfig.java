package org.zalando.nakadi.config;

import org.zalando.nakadi.validation.EventTypeOptionsValidator;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ValidatorConfig {

    @Bean
    public EventTypeOptionsValidator eventTypeOptionsValidator(@Value("${nakadi.topic.min.retentionMs}") final long minTopicRetentionMs,
                                                               @Value("${nakadi.topic.max.retentionMs}") final long maxTopicRetentionMs)
    {
        return new EventTypeOptionsValidator(minTopicRetentionMs, maxTopicRetentionMs);
    }
}
