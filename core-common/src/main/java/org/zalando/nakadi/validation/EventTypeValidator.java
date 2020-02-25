package org.zalando.nakadi.validation;

import org.json.JSONObject;

import java.util.Optional;

public interface EventTypeValidator {

    Optional<ValidationError> validate(JSONObject event);
}
