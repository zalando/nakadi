package org.zalando.nakadi.validation;

import com.google.common.collect.Lists;
import org.json.JSONObject;
import org.zalando.nakadi.validation.schema.JsonAttributeConstraint;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Stack;

public class SchemaCompatibilityChecker {

    final private List<JsonAttributeConstraint> CONSTRAINTS = Lists.newArrayList(
            new JsonAttributeConstraint("not"),
            new JsonAttributeConstraint("patternProperties"));

    public List<SchemaIncompatibility> checkConstraints(final JSONObject schema) {
        final List<SchemaIncompatibility> incompatibilities = new ArrayList<SchemaIncompatibility>();

        recursiveCheckConstraints(schema.toMap(), new Stack<String>(), incompatibilities);

        return incompatibilities;
    }

    private void recursiveCheckConstraints(
            final Map<String, Object> schema,
            final Stack<String> jsonPath,
            final List<SchemaIncompatibility> schemaIncompatibilities) {
        for (final Map.Entry<String, Object> jsonProperty : schema.entrySet()) {


            for (final JsonAttributeConstraint constraint : CONSTRAINTS) {
                final Optional<SchemaIncompatibility> incompatibility = constraint.validate(jsonPath, jsonProperty);
                if (incompatibility.isPresent()) {
                    schemaIncompatibilities.add(incompatibility.get());
                }
            }

            if (jsonProperty.getValue() instanceof Map) {
                jsonPath.push(jsonProperty.getKey());
                recursiveCheckConstraints((Map<String, Object>) jsonProperty.getValue(), jsonPath, schemaIncompatibilities);
                jsonPath.pop();
            }
        }
    }
}
