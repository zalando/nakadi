package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public class EventValidation {
    public static EventTypeValidator forType(final EventType eventType) {
        final EventTypeValidator etv = new EventTypeValidator(eventType);
        final ValidationStrategyConfiguration vsc = new ValidationStrategyConfiguration();
        vsc.setStrategyName(EventBodyMustRespectSchema.NAME);

        // TODO configure metadata validation on a per EventCategory basis

        return etv.withConfiguration(vsc);
    }

    public static JSONObject effectiveSchema(final EventType eventType) throws JSONException {
        final JSONObject schema = new JSONObject(eventType.getSchema().getSchema());

        switch (eventType.getCategory()) {
            case BUSINESS: return addMetadata(schema);
            case DATA: return wrapSchemaInData(schema);
            default: return schema;
        }
    }

    private static JSONObject wrapSchemaInData(final JSONObject schema) {
        final JSONObject wrapper = new JSONObject();

        normalizeSchema(wrapper);

        addMetadata(wrapper);

        wrapper.getJSONObject("properties").put("data_type", new JSONObject("{\"type\": \"string\"}"));

        wrapper.getJSONObject("properties").put("data_op", new JSONObject("{\"type\": \"string\", \"enum\": [\"C\", \"U\", \"D\", \"S\"]}"));

        wrapper.getJSONObject("properties").put("data", schema);

        wrapper.put("additionalProperties", false);

        addToRequired(wrapper, new String[]{ "data_type", "data_op", "data" });

        return wrapper;
    }

    private static JSONObject addMetadata(final JSONObject schema) {
        normalizeSchema(schema);

        schema.getJSONObject("properties").put("metadata", new JSONObject("{\"type\": \"object\"}"));

        addToRequired(schema, new String[]{ "metadata" });

        return schema;
    }

    private static void addToRequired(final JSONObject schema, final String[] toBeRequired) {
        final Set<String> required = new HashSet<>(Arrays.asList(toBeRequired));

        JSONArray currentRequired = schema.getJSONArray("required");

        for(int i = 0; i < currentRequired.length(); i++) {
            required.add(currentRequired.getString(i));
        }

        schema.put("required", required);
    }

    private static void normalizeSchema(final JSONObject schema) {
        schema.put("type", "object");

        if (!schema.has("properties")) {
            schema.put("properties", new JSONObject());
        }

        if (!schema.has("required")) {
            schema.put("required", new JSONArray());
        }
    }
}

