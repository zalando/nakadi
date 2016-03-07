package de.zalando.aruha.nakadi.validation;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import de.zalando.aruha.nakadi.domain.EventType;

import java.util.Map;

public class EventValidation {

    private static final Map<String, EventTypeValidator> eventTypeValidators = Maps.newConcurrentMap();

    public static EventTypeValidator forType(final EventType eventType) {
        return forType(eventType, false);
    }

    public static EventTypeValidator forType(final EventType eventType, final boolean incrementalEdit) {

        final boolean contains = eventTypeValidators.containsKey(eventType.getName());
        Preconditions.checkState(incrementalEdit || !contains, "Validator for EventType {} already defined",
            eventType.getName());

        if (!contains) {
            final EventTypeValidator etsv = new EventTypeValidator(eventType);
            eventTypeValidators.put(eventType.getName(), etsv);
        }

        return eventTypeValidators.get(eventType.getName());
    }

    public static EventTypeValidator lookup(final EventType eventType) {
        return eventTypeValidators.get(eventType.getName());
    }

    public static void reset() {
        eventTypeValidators.clear();
    }
}

