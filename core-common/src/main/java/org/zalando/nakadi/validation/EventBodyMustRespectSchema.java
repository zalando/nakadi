package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.everit.json.schema.Schema;
import org.everit.json.schema.ValidationException;
import org.everit.json.schema.loader.SchemaLoader;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;

import java.util.Optional;

@Component
public class EventBodyMustRespectSchema extends ValidationStrategy {

    private final JsonSchemaEnrichment loader;

    @Autowired
    public EventBodyMustRespectSchema(final JsonSchemaEnrichment loader) {
        this.loader = loader;
    }

    @Override
    public EventValidator materialize(final EventType eventType) {

        final JSONObject effectiveSchema = this.loader.effectiveSchema(eventType);

        final JSONSchemaValidator defaultSchemaValidator = new JSONSchemaValidator(effectiveSchema);

        return defaultSchemaValidator;
    }
}

class JSONSchemaValidator implements EventValidator {

    private final Schema schema;

    private static final FormatValidator DATE_TIME_VALIDATOR = new RFC3339DateTimeValidator();

    JSONSchemaValidator(final JSONObject effectiveSchema) {
        schema = SchemaLoader
                .builder()
                .schemaJson(effectiveSchema)
                .addFormatValidator("date-time", DATE_TIME_VALIDATOR)
                .build()
                .load()
                .build();
    }

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        try {
            schema.validate(event);

            return Optional.empty();
        } catch (final ValidationException e) {
            final StringBuilder builder = new StringBuilder();
            collectErrorMessages(e, builder);

            return Optional.of(new ValidationError(builder.toString()));
        }
    }

    private void collectErrorMessages(final ValidationException e, final StringBuilder builder) {
        builder.append(e.getMessage());

        e.getCausingExceptions().forEach(causingException -> {
            builder.append("\n");
            collectErrorMessages(causingException, builder);
        });
    }
}
