package de.zalando.aruha.nakadi.validation;

import org.json.JSONObject;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

public class FieldNameMustBeSet extends ValidationStrategy {

    public static final String NAME = "field-name-must-be-set";
    private final EventValidator executor = new EventValidator() {

        @Override
        public boolean isValidFor(final JSONObject event) {
            return event.get("name") != null;
        }

    };

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {

        return executor;
    }

}
