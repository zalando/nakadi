package org.zalando.nakadi.config;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;
import com.google.common.io.Resources;
import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.zalando.nakadi.validation.EventTypeOptionsValidator;
import org.zalando.nakadi.validation.SchemaEvolutionService;
import org.zalando.nakadi.validation.schema.CompatibilityModeChangeConstraint;
import org.zalando.nakadi.validation.schema.DeprecatedSchemaChangeConstraint;
import org.zalando.nakadi.validation.schema.PartitionKeyFieldsConstraint;
import org.zalando.nakadi.validation.schema.PartitionStrategyConstraint;
import org.zalando.nakadi.validation.schema.SchemaEvolutionConstraint;

import java.io.IOException;
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
    public SchemaEvolutionService schemaEvolutionService() throws IOException {
        final JSONObject metaSchemaJson = new JSONObject(Resources.toString(Resources.getResource("schema.json"),
                Charsets.UTF_8));
        final Schema metaSchema = SchemaLoader.load(metaSchemaJson);

        final List<SchemaEvolutionConstraint> schemaEvolutionConstraints = Lists.newArrayList(
                new CompatibilityModeChangeConstraint(),
                new DeprecatedSchemaChangeConstraint(),
                new PartitionKeyFieldsConstraint(),
                new PartitionStrategyConstraint()
        );

        return new SchemaEvolutionService(metaSchema, schemaEvolutionConstraints);
    }
}
