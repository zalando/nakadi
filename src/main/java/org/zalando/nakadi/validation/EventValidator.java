package org.zalando.nakadi.validation;

import java.util.Optional;
import org.json.JSONObject;

interface EventValidator {
    Optional<ValidationError> accepts(JSONObject event);
}
