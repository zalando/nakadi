package org.zalando.nakadi.validation.schema;

import com.google.common.collect.Lists;
import org.json.JSONArray;
import org.json.JSONObject;
import org.skyscreamer.jsonassert.JSONCompare;
import org.skyscreamer.jsonassert.JSONCompareMode;

public class SchemaGeneration {
    public JSONObject mergeSchema(final JSONObject thisSchema, final JSONObject thatSchema) {
        if (thisSchema.length() == 0) {
            return thatSchema;
        } else if (JSONCompare.compareJSON(thisSchema, thatSchema, JSONCompareMode.STRICT).failed()) {
            if (thisSchema.getString("type").equals(thatSchema.getString("type"))
                    && thisSchema.getString("type").equals("object")) {
                mergeProperties(thisSchema, thatSchema);
            } else {
                return new JSONObject().put("anyOf", new JSONArray(Lists.newArrayList(thisSchema, thatSchema)));
            }
            return thisSchema;
        } else {
            return thisSchema;
        }
    }

    private void mergeProperties(final JSONObject thisSchema, final JSONObject thatSchema) {
        if (thisSchema.has("properties") || thatSchema.has("properties")) {
            if (!thisSchema.has("properties")) {
                thisSchema.put("properties", new JSONObject());
            } else if (!thatSchema.has("properties")){
                thatSchema.put("properties", new JSONObject());
            }

            for (final String key : thisSchema.getJSONObject("properties").keySet()) {
                if (thatSchema.getJSONObject("properties").has(key)) {
                    final JSONObject mergedSchema = mergeSchema(thisSchema.getJSONObject("properties")
                                    .getJSONObject(key),
                                    thatSchema.getJSONObject("properties").getJSONObject(key));
                    thisSchema.getJSONObject("properties").put(key, mergedSchema);
                }
            }

            for (final String key : thatSchema.getJSONObject("properties").keySet()) {
                if (!thisSchema.getJSONObject("properties").has(key)) {
                    thisSchema.getJSONObject("properties").put(key,
                            thatSchema.getJSONObject("properties").getJSONObject(key));
                }
            }
        }
    }

    public JSONObject schemaFor(final Object event) {
        if (event instanceof String) {
            return new JSONObject().put("type", "string");
        } else if (event instanceof Integer || event instanceof Double) {
            return new JSONObject().put("type", "number");
        } else if (event instanceof Boolean) {
            return new JSONObject().put("type", "boolean");
        } else if (event == JSONObject.NULL) {
            return new JSONObject().put("type", "null");
        } else if (event instanceof JSONArray) {
            return schemaFor((JSONArray) event);
        } else {
            return schemaFor((JSONObject) event);
        }
    }

    private JSONObject schemaFor(final JSONObject event) {
        final JSONObject schema = new JSONObject();
        schema.put("type", "object");

        if (event.length() > 0) {
            final JSONObject properties = new JSONObject();
            for (final String key : event.keySet()) {
                properties.put(key, schemaFor(event.get(key)));
            }
            schema.put("properties", properties);
        }

        return schema;
    }

    private JSONObject schemaFor(final JSONArray event) {
        final JSONObject schema = new JSONObject();
        schema.put("type", "array");

        if (event.length() > 0) {
            JSONObject itemSchema = new JSONObject();
            for (int i = 0; i < event.length(); i = i + 1) {
                final JSONObject thatSchema = schemaFor(event.get(i));
                itemSchema = mergeSchema(itemSchema, thatSchema);
            }
            schema.put("items", itemSchema);
        }

        return schema;
    }
}
