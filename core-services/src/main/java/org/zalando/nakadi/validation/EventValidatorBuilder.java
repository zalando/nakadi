package org.zalando.nakadi.validation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@Component
public class EventValidatorBuilder {

    private final List<ValidationStrategy> strategies;

    @Autowired
    public EventValidatorBuilder(final List<ValidationStrategy> eventValidators) {
        this.strategies = eventValidators;
    }

    public EventTypeValidator build(final EventType eventType) {
        final List<EventValidator> validatorList = strategies.stream()
                .map(e -> e.createValidator(eventType))
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
        return instantiateValidator(validatorList);
    }

    private EventTypeValidator instantiateValidator(final List<EventValidator> validatorList) {
        return (event) -> validatorList
                .stream()
                .map(validator -> validator.accepts(event))
                .filter(Optional::isPresent)
                .findFirst()
                .orElse(Optional.empty());
    }
}

