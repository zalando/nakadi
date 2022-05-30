package org.zalando.nakadi.validation;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventTypeSchema;

import java.util.Optional;

public interface JsonSchemaValidator {

    Optional<ValidationError> validate(JSONObject event);

    // Returns the schema which is used for event validation.
    EventTypeSchema getSchema();
}
