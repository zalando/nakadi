package org.zalando.nakadi.validation;

import org.json.JSONObject;

import java.util.Optional;

interface EventValidator {
    Optional<ValidationError> accepts(JSONObject event);
}
