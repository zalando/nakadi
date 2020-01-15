package org.zalando.nakadi.validation;

import org.json.JSONObject;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;

import java.util.Optional;

public class FieldNameMustBeSet extends ValidationStrategy {

    public static final String NAME = "field-name-must-be-set";
    private final EventValidator executor = new EventValidator() {

        @Override
        public Optional<ValidationError> accepts(final JSONObject event) {
            if (event.has("name") && event.get("name") != null) {
                return Optional.empty();
            } else {
                return Optional.of(new ValidationError("name is required"));
            }
        }
    };

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {

        return executor;
    }

}
