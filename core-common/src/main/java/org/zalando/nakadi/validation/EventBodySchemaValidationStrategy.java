package org.zalando.nakadi.validation;

import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;

import java.util.Optional;

@Component
public class EventBodySchemaValidationStrategy implements ValidationStrategy {

    private final JsonSchemaEnrichment loader;

    @Autowired
    public EventBodySchemaValidationStrategy(final JsonSchemaEnrichment loader) {
        this.loader = loader;
    }

    @Override
    public EventValidator createValidator(final EventType eventType) {
        final Schema schema = SchemaLoader.builder()
                .schemaJson(loader.effectiveSchema(eventType))
                .addFormatValidator(new RFC3339DateTimeValidator())
                .build()
                .load()
                .build();
        return (evt) -> validate(schema, evt);
    }

    private Optional<ValidationError> validate(final Schema schema, final JSONObject event) {
        try {
            schema.validate(event);
            return Optional.empty();
        } catch (final ValidationException e) {
            final StringBuilder builder = new StringBuilder();
            recursiveCollectErrors(e, builder);

            return Optional.of(new ValidationError(builder.toString()));
        }
    }

    private void recursiveCollectErrors(final ValidationException e, final StringBuilder builder) {
        builder.append(e.getMessage());

        e.getCausingExceptions().forEach(causingException -> {
            builder.append("\n");
            recursiveCollectErrors(causingException, builder);
        });
    }
}
