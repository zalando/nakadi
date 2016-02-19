package de.zalando.aruha.nakadi.validation;

import org.json.JSONObject;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

import java.util.Optional;

public class FieldNameMustBeSet extends ValidationStrategy {

    public static final String NAME = "field-name-must-be-set";
    private final EventValidator executor = new EventValidator() {

        @Override
        public Optional<ValidationError> accepts(final JSONObject event) {
            if (event.get("name") == null) {
                return Optional.of(new ValidationError("name is required"));
            } else {
                return Optional.empty();
            }
        }
    };

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {

        return executor;
    }

}
