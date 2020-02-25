package org.zalando.nakadi.validation;

import com.google.common.collect.ImmutableList;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventTypeBase;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class JsonSchemaEnrichment {
    public static final String DATA_CHANGE_WRAP_FIELD = "data";
    public static final String DATA_PATH_PREFIX = JsonSchemaEnrichment.DATA_CHANGE_WRAP_FIELD + ".";

    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String ADDITIONAL_ITEMS = "additionalItems";
    private static final List<String> NESTED_SCHEMA_KEYWORDS = ImmutableList.of("definitions", "dependencies",
            "properties");
    private static final List<String> OBJECT_SCHEMA_KEYWORDS = ImmutableList.of("properties", "required",
            "minProperties", "maxProperties");
    private static final List<String> ARRAY_SCHEMA_KEYWORDS = ImmutableList.of("minItems", "maxItems", "uniqueItems",
            "items");
    private static final List<String> COMPOSED_SCHEMA_KEYWORDS = ImmutableList.of("anyOf", "allOf", "oneOf");

    public JSONObject effectiveSchema(final EventTypeBase eventType) throws JSONException {
        final JSONObject schema = new JSONObject(eventType.getSchema().getSchema());

        if (eventType.getCompatibilityMode().equals(CompatibilityMode.COMPATIBLE)) {
            this.enforceStrictValidation(schema);
        }

        switch (eventType.getCategory()) {
            case BUSINESS:
                return addMetadata(schema, eventType);
            case DATA:
                return wrapSchemaInData(schema, eventType);
            default:
                return schema;
        }
    }

    private void enforceStrictValidation(final JSONObject schema) {
        enforceNoAdditionalProperties(schema);
        enforceNoAdditionalItems(schema);

        COMPOSED_SCHEMA_KEYWORDS.forEach(keyword -> {
            if (schema.has(keyword)) {
                schema.getJSONArray(keyword)
                        .forEach(object -> enforceStrictValidation((JSONObject) object));
            }
        });
    }

    private void enforceNoAdditionalItems(final JSONObject schema) {
        Optional.ofNullable(schema.optString("type")).map(type -> type.equals("array")).filter(b -> b)
                .ifPresent(b -> schema.put(ADDITIONAL_ITEMS, false));

        Optional.ofNullable(schema.optJSONArray("type")).map(array -> array.toList().contains("array"))
                .filter(b -> b).ifPresent(b -> schema.put(ADDITIONAL_ITEMS, false));

        Optional.ofNullable(schema.opt("items")).ifPresent(items -> schema.put(ADDITIONAL_ITEMS, false));

        Optional.ofNullable(schema.optJSONArray("items"))
                .ifPresent(items -> items.forEach(item -> enforceStrictValidation((JSONObject) item)));

        Optional.ofNullable(schema.optJSONObject("items")).ifPresent(this::enforceStrictValidation);

        ARRAY_SCHEMA_KEYWORDS.forEach(keyword -> {
            if (schema.has(keyword)) {
                schema.put(ADDITIONAL_ITEMS, false);
            }
        });
    }

    private void enforceNoAdditionalProperties(final JSONObject schema) {
        if (isEmptySchema(schema)) {
            schema.put(ADDITIONAL_PROPERTIES, false);
        }

        Optional.ofNullable(schema.optString("type")).map(type -> type.equals("object")).filter(b -> b)
                .ifPresent(b -> schema.put(ADDITIONAL_PROPERTIES, false));

        Optional.ofNullable(schema.optJSONArray("type")).map(array -> array.toList().contains("object"))
                .filter(b -> b).ifPresent(b -> schema.put(ADDITIONAL_PROPERTIES, false));

        OBJECT_SCHEMA_KEYWORDS.forEach(keyword -> {
            if (schema.has(keyword)) {
                schema.put(ADDITIONAL_PROPERTIES, false);
            }
        });

        NESTED_SCHEMA_KEYWORDS.forEach(keyword -> {
            Optional.ofNullable(schema.optJSONObject(keyword))
                    .ifPresent(object ->
                            object.keySet().forEach(key -> enforceStrictValidation(object.getJSONObject(key)))
                    );
        });
    }

    private boolean isEmptySchema(final JSONObject schema) {
        return !(
                OBJECT_SCHEMA_KEYWORDS.stream().anyMatch(schema::has) ||
                        ARRAY_SCHEMA_KEYWORDS.stream().anyMatch(schema::has) ||
                        COMPOSED_SCHEMA_KEYWORDS.stream().anyMatch(schema::has) ||
                        schema.has("$ref") ||
                        schema.has("type")
        );
    }

    private static JSONObject wrapSchemaInData(final JSONObject schema, final EventTypeBase eventType) {
        final JSONObject wrapper = new JSONObject();

        normalizeSchema(wrapper);

        addMetadata(wrapper, eventType);

        moveDefinitionsToRoot(wrapper, schema);

        final JSONObject properties = wrapper.getJSONObject("properties");

        properties.put("data_type", new JSONObject().put("type", "string"));
        properties.put("data_op", new JSONObject().put("type", "string")
                .put("enum", Arrays.asList("C", "U", "D", "S")));
        properties.put(DATA_CHANGE_WRAP_FIELD, schema);

        wrapper.put(ADDITIONAL_PROPERTIES, false);

        addToRequired(wrapper, new String[]{"data_type", "data_op", "data"});

        return wrapper;
    }

    private static void moveDefinitionsToRoot(final JSONObject wrapper, final JSONObject schema) {
        final Object definitions = schema.remove("definitions");

        if (definitions != null) {
            wrapper.put("definitions", definitions);
        }
    }

    private static JSONObject addMetadata(final JSONObject schema, final EventTypeBase eventType) {
        normalizeSchema(schema);
        final JSONObject metadata = createMetadata(eventType.getName(), eventType.getCleanupPolicy());

        schema.getJSONObject("properties").put("metadata", metadata);
        addToRequired(schema, new String[]{"metadata"});
        return schema;
    }

    public static JSONObject createMetadata(final String eventTypeName, final CleanupPolicy cleanupPolicy) {
        final JSONObject result = loadResource("schema_metadata.json");
        if (cleanupPolicy == CleanupPolicy.COMPACT) {
            JSONTools.extendJson(
                    result,
                    loadResource("schema_metadata_compact_extension.json"));
        }
        JSONTools.replaceAll(result, "{{EVENT_TYPE_NAME}}", eventTypeName);
        return result;
    }

    private static JSONObject loadResource(final String resource) {
        InputStream in = JsonSchemaEnrichment.class.getClassLoader().getResourceAsStream(resource);
        if (null == in) {
            // Resource is not bundled, probably external file
            try {
                in = new FileInputStream(resource);
            } catch (final FileNotFoundException e) {
                throw new RuntimeException("Failed to find resource " + resource, e);
            }
        }
        try {
            return new JSONObject(new JSONTokener(in));
        } finally {
            try {
                in.close();
            } catch (final IOException ignore) {
                // Failed to close file, but it's fine. probably...
            }
        }
    }

    private static void addToRequired(final JSONObject schema, final String[] toBeRequired) {
        final Set<String> required = new HashSet<>(Arrays.asList(toBeRequired));

        final JSONArray currentRequired = schema.getJSONArray("required");

        for (int i = 0; i < currentRequired.length(); i++) {
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
