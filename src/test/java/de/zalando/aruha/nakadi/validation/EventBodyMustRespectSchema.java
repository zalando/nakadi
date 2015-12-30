package de.zalando.aruha.nakadi.validation;

import javax.validation.ValidationException;

import org.everit.json.schema.Schema;
import org.everit.json.schema.loader.SchemaLoader;

import org.json.JSONObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

public class EventBodyMustRespectSchema extends ValidationStrategy {

    public static final String NAME = "event-body-must-respect-schema";

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {
        final JSONObject effectiveSchema = vsc.getAdditionalConfiguration() != null
            ? ignoreFieldsOf(eventType.getEventTypeSchema().getSchema(), vsc.getAdditionalConfiguration())
            : eventType.getEventTypeSchema().getSchema();
        return new JSONSchemaValidator(effectiveSchema);

    }

    private JSONObject ignoreFieldsOf(final JSONObject schema, final JSONObject additionalConfiguration) {
        throw new UnsupportedOperationException();
    }
}

class JSONSchemaValidator implements EventValidator {

    private static final Logger LOG = LoggerFactory.getLogger(JSONSchemaValidator.class);

    private final Schema schema;

    public JSONSchemaValidator(final JSONObject effectiveSchema) {
        schema = SchemaLoader.load(effectiveSchema);
    }

    @Override
    public boolean isValidFor(final JSONObject event) {
        try {
            schema.validate(event);
            return true;
        } catch (final ValidationException e) {
            LOG.info("Validation failed", e);
            return false;
        }
    }
}
