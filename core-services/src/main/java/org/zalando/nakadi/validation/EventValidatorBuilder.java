package org.zalando.nakadi.validation;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

@Component
public class EventValidatorBuilder {

    private final RFC3339DateTimeValidator dateTimeValidator = new RFC3339DateTimeValidator();
    private final JsonSchemaEnrichment loader;

    @Autowired
    public EventValidatorBuilder(
            final JsonSchemaEnrichment loader) {
        this.loader = loader;
    }

    public EventTypeValidator build(final EventType eventType) {
        final List<Function<JSONObject, Optional<ValidationError>>> validators = new ArrayList<>();

        // 1. We always validate schema.
        final Schema schema = SchemaLoader.builder()
                .schemaJson(loader.effectiveSchema(eventType))
                .addFormatValidator(new RFC3339DateTimeValidator())
                .build()
                .load()
                .build();
        validators.add((evt) -> validateSchemaConformance(schema, evt));

        // 2. in case of data or business event type we validate occurred_at
        if (eventType.getCategory() == EventCategory.DATA || eventType.getCategory() == EventCategory.BUSINESS) {
            validators.add(this::validateOccurredAt);
        }

        return (event) -> validators
                .stream()
                .map(validator -> validator.apply(event))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());
    }

    private Optional<ValidationError> validateOccurredAt(final JSONObject event) {
        return Optional
                .ofNullable(event.optJSONObject("metadata"))
                .map(metadata -> metadata.optString("occurred_at"))
                .flatMap(dateTimeValidator::validate)
                .map(e -> new ValidationError("#/metadata/occurred_at:" + e));

    }

    private Optional<ValidationError> validateSchemaConformance(final Schema schema, final JSONObject evt) {
        try {
            schema.validate(evt);
            return Optional.empty();
        } catch (final ValidationException e) {
            final StringBuilder builder = new StringBuilder();
            recursiveCollectErrors(e, builder);
            return Optional.of(new ValidationError(builder.toString()));
        }
    }

    private static void recursiveCollectErrors(final ValidationException e, final StringBuilder builder) {
        builder.append(e.getMessage());

        e.getCausingExceptions().forEach(causingException -> {
            builder.append("\n");
            recursiveCollectErrors(causingException, builder);
        });
    }

}

