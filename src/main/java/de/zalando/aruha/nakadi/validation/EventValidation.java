package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

public class EventValidation {
    public static EventTypeValidator forType(final EventType eventType) {
        return new EventTypeValidator(eventType);
    }

    public static JSONObject effectiveSchema(EventType eventType) throws JSONException {
        final JSONObject schema = new JSONObject(eventType.getSchema().getSchema());

        switch (eventType.getCategory()) {
            case BUSINESS: {
                return addMetadata(schema);
            }
            default: return schema;
        }
    }

    private static JSONObject addMetadata(JSONObject schema) {
        final JSONObject metadata = new JSONObject("{\"type\": \"object\"}");
        schema.put("type", "object");

        if (!schema.has("properties")) {
            schema.put("properties", new HashMap());
        }

        if (!schema.has("required")) {
            schema.put("required", new JSONArray());
        }

        schema.getJSONObject("properties").put("metadata", metadata);

        Set required = new HashSet<String>();

        for(int i = 0; i < schema.getJSONArray("required").length(); i++) {
            required.add(schema.getJSONArray("required").get(i));
        }

        required.add("metadata");

        schema.put("required", required);

        return schema;
    }
}

