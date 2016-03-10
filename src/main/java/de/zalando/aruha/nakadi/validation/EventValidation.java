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

        return etv.withConfiguration(vsc);
    }

    public static JSONObject effectiveSchema(final EventType eventType) throws JSONException {
        final JSONObject schema = new JSONObject(eventType.getSchema().getSchema());

        switch (eventType.getCategory()) {
            case BUSINESS: return addMetadata(schema, eventType);
            case DATA: return wrapSchemaInData(schema, eventType);
            default: return schema;
        }
    }

    private static JSONObject wrapSchemaInData(final JSONObject schema, final EventType eventType) {
        final JSONObject wrapper = new JSONObject();

        normalizeSchema(wrapper);

        addMetadata(wrapper, eventType);

        final JSONObject properties = wrapper.getJSONObject("properties");

        properties.put("data_type", new JSONObject().put("type", "string"));
        properties.put("data_op", new JSONObject().put("type", "string")
                .put("enum", Arrays.asList(new String[] { "C", "U", "D", "S" })));
        properties.put("data", schema);

        wrapper.put("additionalProperties", false);

        addToRequired(wrapper, new String[]{ "data_type", "data_op", "data" });

        return wrapper;
    }

    private static JSONObject addMetadata(final JSONObject schema, final EventType eventType) {
        normalizeSchema(schema);

        final JSONObject metadata = new JSONObject();
        final JSONObject metadataProperties = new JSONObject();

        final JSONObject uuid = new JSONObject()
                .put("type", "string")
                .put("pattern", "^[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}$");
        final JSONObject arrayOfUUIDs = new JSONObject()
                .put("type", "array")
                .put("items", uuid);
        final JSONObject eventTypeString = new JSONObject()
                .put("type", "string")
                .put("enum", Arrays.asList(new String[] { eventType.getName() }));
        final JSONObject string = new JSONObject().put("type", "string");
        final JSONObject dateTime = new JSONObject()
                .put("type", "string")
                .put("pattern", "^[0-9]{4}-[0-9]{2}-[0-9]{2}(T| )[0-9]{2}:[0-9]{2}:[0-9]{2}(.[0-9]+)?(Z|[+-][0-9]{2}:[0-9]{2})$");

        metadataProperties.put("eid", uuid);
        metadataProperties.put("event_type", eventTypeString);
        metadataProperties.put("occurred_at", dateTime);
        metadataProperties.put("parent_eids", arrayOfUUIDs);
        metadataProperties.put("flow_id", string);

        metadata.put("type", "object");
        metadata.put("properties", metadataProperties);
        metadata.put("required", Arrays.asList(new String[]{"eid", "occurred_at"}));
        metadata.put("additionalProperties", false);

        schema.getJSONObject("properties").put("metadata", metadata);

        addToRequired(schema, new String[]{ "metadata" });

        return schema;
    }

    private static void addToRequired(final JSONObject schema, final String[] toBeRequired) {
        final Set<String> required = new HashSet<>(Arrays.asList(toBeRequired));

        final JSONArray currentRequired = schema.getJSONArray("required");

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

