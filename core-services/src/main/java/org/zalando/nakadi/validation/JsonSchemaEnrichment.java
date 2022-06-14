package org.zalando.nakadi.validation;

import com.google.common.collect.ImmutableList;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.io.Resource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.CleanupPolicy;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventTypeBase;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@Component
public class JsonSchemaEnrichment {

    private static final String ADDITIONAL_PROPERTIES = "additionalProperties";
    private static final String ADDITIONAL_ITEMS = "additionalItems";
    private static final List<String> NESTED_SCHEMA_KEYWORDS = ImmutableList.of("definitions", "dependencies",
            "properties");
    private static final List<String> OBJECT_SCHEMA_KEYWORDS = ImmutableList.of("properties", "required",
            "minProperties", "maxProperties");
    private static final List<String> ARRAY_SCHEMA_KEYWORDS = ImmutableList.of("minItems", "maxItems", "uniqueItems",
            "items");
    private static final List<String> COMPOSED_SCHEMA_KEYWORDS = ImmutableList.of("anyOf", "allOf", "oneOf");
    private final Map<CleanupPolicy, JSONObject> metadataSchemas = new HashMap<>();

    @Autowired
    public JsonSchemaEnrichment(
            final ResourceLoader resourceLoader,
            @Value("${nakadi.schema.metadata:classpath:schema_metadata.json}") final String metadataSchemaResource)
            throws IOException {
        final Resource resource = resourceLoader.getResource(metadataSchemaResource);
        try (InputStream in = resource.getInputStream()) {
            final JSONObject schemas = new JSONObject(new JSONTokener(new InputStreamReader(in)));
            for (final CleanupPolicy cp : CleanupPolicy.values()) {
                metadataSchemas.put(cp, schemas.getJSONObject(cp.name().toLowerCase()));
            }
        }
    }

    public JSONObject effectiveSchema(final EventTypeBase eventType, final String schemaString) {
        final JSONObject schema = new JSONObject(schemaString);
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

    private JSONObject wrapSchemaInData(final JSONObject schema, final EventTypeBase eventType) {
        final JSONObject wrapper = new JSONObject();

        normalizeSchema(wrapper);

        addMetadata(wrapper, eventType);

        moveDefinitionsToRoot(wrapper, schema);

        final JSONObject properties = wrapper.getJSONObject("properties");

        properties.put("data_type", new JSONObject().put("type", "string"));
        properties.put("data_op", new JSONObject().put("type", "string")
                .put("enum", Arrays.asList("C", "U", "D", "S")));
        properties.put(EventTypeBase.DATA_CHANGE_WRAP_FIELD, schema);

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

    private JSONObject addMetadata(final JSONObject schema, final EventTypeBase eventType) {
        normalizeSchema(schema);
        final JSONObject metadata = createMetadata(eventType.getName(), eventType.getCleanupPolicy());

        schema.getJSONObject("properties").put("metadata", metadata);
        addToRequired(schema, new String[]{"metadata"});
        return schema;
    }

    public JSONObject createMetadata(final String eventTypeName, final CleanupPolicy cleanupPolicy) {
        // We are creating a copy of metadata object, as we are modifying it afterwards.
        final JSONObject metadataSchema = new JSONObject(metadataSchemas.get(cleanupPolicy).toString());

        metadataSchema
                .getJSONObject("properties")
                .getJSONObject("event_type")
                .put("enum", new JSONArray(Collections.singleton(eventTypeName)));
        return metadataSchema;
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
