package org.zalando.nakadi.validation.schema;

import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;

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
        final JSONArray events = new JSONArray(readFile("org/zalando/nakadi/validation/spp.events.json"));

        JSONObject schema = new JSONObject();

        for (final Object event : events) {
            final JSONObject eventSchema = service.schemaFor(event);
            schema = service.mergeSchema(schema, eventSchema);
        }

        System.out.println(schema.toString());

        printKSQLTableSchema(schema);
    }

    private void printKSQLTableSchema(final JSONObject schema) {
        int counter = 1;
        final int size = schema.getJSONObject("properties").keySet().size();
        for (final String key : schema.getJSONObject("properties").keySet()) {
            System.out.print(key.replace('.', '_').replace('-', '_') + " ");
            printKSQLType(schema.getJSONObject("properties").getJSONObject(key));
            if (counter < size) {
                System.out.print(",");
            }
            counter = counter + 1;
        }
    }

    private void printKSQLType(final JSONObject jsonObject) {
        switch (jsonObject.getString("type")) {
            case "string":
                System.out.print("VARCHAR");
                break;
            case "number":
                System.out.print("DOUBLE");
                break;
            case "boolean":
                System.out.print("BOOLEAN");
                break;
            case "null":
                System.out.print("VARCHAR");
                break;
            case "object":
                System.out.print("STRUCT<");
                printKSQLTableSchema(jsonObject);
                System.out.print(">");
                break;
            case "array":
                System.out.print("ARRAY<");
                printKSQLType(jsonObject.getJSONObject("items"));
                System.out.print(">");
                break;
            default:
                throw new RuntimeException();
        }
    }

}