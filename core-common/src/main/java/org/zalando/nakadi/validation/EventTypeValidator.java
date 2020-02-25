package org.zalando.nakadi.validation;

import org.json.JSONObject;

import java.util.List;
import java.util.Optional;

public class EventTypeValidator {

    private final List<EventValidator> validators;

    public EventTypeValidator(
            final List<EventValidator> eventValidators) {
        this.validators = eventValidators;
    }

    public Optional<ValidationError> validate(final JSONObject event) {
        return validators
                .stream()
                .map(validator -> validator.accepts(event))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());
    }
}
