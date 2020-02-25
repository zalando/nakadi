package org.zalando.nakadi.validation;

import org.json.JSONObject;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;

import java.util.Optional;

@Component
public class EventMetadataValidationStrategy implements ValidationStrategy {
    private final RFC3339DateTimeValidator dateTimeValidator = new RFC3339DateTimeValidator();

    @Override
    public EventValidator createValidator(final EventType eventType) {
        if (eventType.getCategory() == EventCategory.DATA || eventType.getCategory() == EventCategory.BUSINESS) {
            return this::validate;
        }
        return null;
    }

    private Optional<ValidationError> checkDateTime(final String occurredAt) {
        return dateTimeValidator.validate(occurredAt)
                .map(e-> new ValidationError("#/metadata/occurred_at:" + e));
    }

    private Optional<ValidationError> validate(final JSONObject event) {
        return Optional
                .ofNullable(event.optJSONObject("metadata"))
                .map(metadata -> metadata.optString("occurred_at"))
                .flatMap(this::checkDateTime);

    }
}
