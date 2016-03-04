package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;

public class EventValidation {
    public static EventTypeValidator forType(final EventType eventType) {
        return new EventTypeValidator(eventType);
    }
}

