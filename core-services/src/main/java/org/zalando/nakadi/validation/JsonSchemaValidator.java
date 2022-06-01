package org.zalando.nakadi.validation;

import org.json.JSONObject;

import java.util.Optional;

public interface JsonSchemaValidator {

    Optional<ValidationError> validate(JSONObject event);

}
