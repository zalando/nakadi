package org.zalando.nakadi.config;

import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.schema.AdditionalPropertiesConstraint;
import org.zalando.nakadi.validation.schema.AdditionalItemsConstraint;
import org.zalando.nakadi.validation.schema.CompatibilityModeChangeConstraint;
import org.zalando.nakadi.validation.schema.CompatibleSchemaChangeConstraint;
import org.zalando.nakadi.validation.schema.DeprecatedSchemaChangeConstraint;
import org.zalando.nakadi.validation.schema.NotSchemaConstraint;
import org.zalando.nakadi.validation.schema.PatternPropertiesConstraint;
import org.zalando.nakadi.validation.schema.SchemaConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;

import java.util.List;

@Configuration
public class ValidatorConfig {

    @Bean
    public EventTypeOptionsValidator eventTypeOptionsValidator(
            @Value("${nakadi.topic.min.retentionMs}") final long minTopicRetentionMs,
            @Value("${nakadi.topic.max.retentionMs}") final long maxTopicRetentionMs) {
        return new EventTypeOptionsValidator(minTopicRetentionMs, maxTopicRetentionMs);
    }

    @Bean
    public SchemaEvolutionService schemaEvolutionService() {
        final List<SchemaConstraint> constraints = Lists.newArrayList(
                new NotSchemaConstraint(),
                new PatternPropertiesConstraint(),
                new AdditionalPropertiesConstraint(),
                new AdditionalItemsConstraint()
        );

        final List<SchemaEvolutionConstraint> schemaEvolutionConstraints = Lists.newArrayList(
                new CompatibilityModeChangeConstraint(),
                new CompatibleSchemaChangeConstraint(),
                new DeprecatedSchemaChangeConstraint()
        );

        return new SchemaEvolutionService(constraints, schemaEvolutionConstraints);
    }
}
