package de.zalando.aruha.nakadi.validation;

import com.google.common.collect.Lists;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import org.json.JSONObject;

import java.util.List;
import java.util.Optional;

public class EventTypeValidator {

    private final EventType eventType;
    private final List<EventValidator> validators = Lists.newArrayList();

    public EventTypeValidator(final EventType eventType) {
        this.eventType = eventType;
    }

    public Optional<ValidationError> validate(final JSONObject event) {
        return validators
                .stream()
                .map(validator -> validator.accepts(event))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());
    }

    public EventTypeValidator withConfiguration(final ValidationStrategyConfiguration vsc) {
        final ValidationStrategy vs = ValidationStrategy.lookup(vsc.getStrategyName());
        validators.add(vs.materialize(eventType, vsc));

        return this;
    }

}
