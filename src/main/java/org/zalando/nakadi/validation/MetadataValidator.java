package org.zalando.nakadi.validation;

import org.json.JSONObject;

import java.util.Optional;

public class MetadataValidator implements EventValidator {

    private final RFC3339DateTimeValidator validator = new RFC3339DateTimeValidator();

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        return Optional
                .ofNullable(event.optJSONObject("metadata"))
                .map(metadata -> metadata.optString("occurred_at"))
                .flatMap(this::checkDateTime);
    }

    private Optional<ValidationError> checkDateTime(final String occurredAt) {
        return validator.validate(occurredAt)
                .map(e-> new ValidationError("#/metadata/occurred_at:" + e));
    }
}
