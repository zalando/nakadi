package org.zalando.nakadi.validation.schema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.utils.TestUtils.readFile;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

public class SchemaGenerationTest {
    private SchemaGeneration service = new SchemaGeneration();

    @Test
    public void schemaFor() throws Exception {
        final JSONArray testCases = new JSONArray(readFile("org/zalando/nakadi/validation/schema-generation.json"));

        for(final Object testCaseObject : testCases) {
            final JSONObject testCase = (JSONObject) testCaseObject;
            final JSONObject event = testCase.getJSONObject("event");
            final JSONObject schema = testCase.getJSONObject("schema");
            final String description = testCase.getString("description");

            assertThat(description, service.schemaFor(event), is(sameJSONObjectAs(schema)));
        }
    }

    @Test
    public void generateComplexSchema() throws Exception {
        final JSONObject schema = new JSONObject(readFile("org/zalando/nakadi/validation/product.baseschema.json"));

        final JSONObject expandedSchema = expandSchema(schema, schema.getJSONObject("definitions"));

        final BufferedWriter writer = new BufferedWriter(new FileWriter("/Users/rdecillo/Desktop/expanded_schema.json"));
        writer.write(expandedSchema.toString());
        writer.close();

        final BufferedWriter writerKSQL = new BufferedWriter(new FileWriter("/Users/rdecillo/Desktop/ksql_type.txt"));
        printKSQLTableSchema(expandedSchema, writerKSQL);
        writerKSQL.close();
    }

    private JSONObject expandSchema(final JSONObject schema, final JSONObject definitions) {
//        System.out.println("process: " + schema.toString());
        if (schema.has("type")) {
            switch (schema.getString("type")) {
                case "string":
                    return schema;
                case "number":
                    return schema;
                case "integer":
                    return new JSONObject().put("type", "number");
                case "boolean":
                    return schema;
                case "object": {
                    return expandObjectSchema(schema, definitions);
                }
                case "array": {
                    final JSONObject newSchema = new JSONObject().put("type", "array");
                    return newSchema.put("items", expandSchema(schema.getJSONObject("items"), definitions));
                }
            }
        } else if (schema.keySet().contains("properties")) { // root level without type definition...
            return expandObjectSchema(schema, definitions);
        } else if (schema.keySet().contains("$ref")) {
            return derreferenceSchema(schema, definitions);
        } else if (schema.keySet().contains("allOf")) {
            return mergeComposedSchema(schema, definitions, "allOf");
        } else if (schema.keySet().contains("oneOf")) {
            return mergeComposedSchema(schema, definitions, "oneOf");
        } else if (schema.keySet().contains("additionalProperties")) { // TODO: additionalProperties
            return schema;
        }

        throw new RuntimeException();
    }

    private JSONObject mergeComposedSchema(final JSONObject schema, final JSONObject definitions, final String compositionType) {
        JSONObject returnSchema = new JSONObject().put("type", "object").put("properties", new JSONObject());
        for (final Object aSchema : schema.getJSONArray(compositionType).toList()) {
            returnSchema = service.mergeSchema(returnSchema, expandSchema(new JSONObject((HashMap) aSchema), definitions));
        }
        return returnSchema;
    }

    private JSONObject derreferenceSchema(final JSONObject schema, final JSONObject definitions) {
        final String ref = schema.getString("$ref");
        JSONObject finalDefinition = definitions;
        for (final String key : ref.split("/")) {
            if (key.equals("#") || key.equals("definitions")) {
                continue;
            }
            finalDefinition = finalDefinition.getJSONObject(key);
        }
        return expandSchema(finalDefinition, definitions);
    }

    private JSONObject expandObjectSchema(final JSONObject schema, final JSONObject definitions) {
        final JSONObject newSchema = new JSONObject().put("type", "object").put("properties", new JSONObject());
        for (final String key : schema.getJSONObject("properties").keySet()) {
            newSchema.getJSONObject("properties")
                    .put(key, expandSchema(schema.getJSONObject("properties").getJSONObject(key), definitions));
        }
        return newSchema;
    }

    private void printKSQLTableSchema(final JSONObject schema, final BufferedWriter writer) throws IOException {
        int counter = 1;
        final int size = schema.getJSONObject("properties").keySet().size();
        for (final String key : schema.getJSONObject("properties").keySet()) {
            writer.write(key.replace('.', '_').replace('-', '_') + " ");
            printKSQLType(schema.getJSONObject("properties").getJSONObject(key), writer);
            if (counter < size) {
                writer.write(",");
            }
            counter = counter + 1;
        }
    }

    private void printKSQLType(final JSONObject jsonObject, final BufferedWriter writer) throws IOException {
        if (jsonObject.has("additionalProperties")) {
            writer.write("MAP<VARCHAR,");
            printKSQLType(jsonObject.getJSONObject("additionalProperties"), writer);
            writer.write(">");
            return;
        }
        switch (jsonObject.getString("type")) {
            case "string":
                writer.write("VARCHAR");
                break;
            case "number":
                writer.write("DOUBLE");
                break;
            case "boolean":
                writer.write("BOOLEAN");
                break;
            case "null":
                writer.write("VARCHAR");
                break;
            case "object":
                writer.write("STRUCT<");
                printKSQLTableSchema(jsonObject, writer);
                writer.write(">");
                break;
            case "array":
                writer.write("ARRAY<");
                printKSQLType(jsonObject.getJSONObject("items"), writer);
                writer.write(">");
                break;
            default:
                throw new RuntimeException();
        }
    }

}