package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONException;
import org.json.JSONObject;

public class EventValidation {
    public static EventTypeValidator forType(final EventType eventType) {
        return new EventTypeValidator(eventType);
    }

    public static JSONObject effectiveSchema(EventType eventType) throws JSONException {
        return new JSONObject(eventType.getSchema().getSchema());
    }
}

