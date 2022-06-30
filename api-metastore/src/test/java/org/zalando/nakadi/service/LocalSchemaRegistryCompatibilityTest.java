package org.zalando.nakadi.service;

import org.apache.avro.SchemaBuilder;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.util.Assert;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.util.AvroUtils;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.NAME_MISMATCH;
import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.READER_FIELD_MISSING_DEFAULT_VALUE;
import static org.apache.avro.SchemaCompatibility.SchemaIncompatibilityType.TYPE_MISMATCH;


public class LocalSchemaRegistryCompatibilityTest {

    private AvroSchemaCompatibility avroSchemaCompatibility;
    private static final String BASE_RECORD_JSON =  getBaseRecordJson();

    @Before
    public void setUp() throws Exception {
        this.avroSchemaCompatibility = new AvroSchemaCompatibility();
    }


    @Test
    public void testAddFieldIsForwardCompatibleChange(){
        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));
        final var newSchema = AvroUtils.getParsedSchema(
                putAndGetJson(
                        new JSONObject("{\"type\":\"string\",\"name\":\"saz\"}"),
                        BASE_RECORD_JSON)
        );
        final var incompatibilities =
                avroSchemaCompatibility.validateSchema(original, newSchema, CompatibilityMode.FORWARD);

        Assert.isTrue(incompatibilities.isEmpty(), "no incompatibilities should exist");
    }

    @Test
    public void testRemoveFieldIsNotForwardCompatibleChange(){
        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));
        final var newSchema = AvroUtils.getParsedSchema(
                removeAndGetJson("baz", BASE_RECORD_JSON)
        );
        final var incompatibilities =
                avroSchemaCompatibility.validateSchema(original, newSchema, CompatibilityMode.FORWARD);
        final var expected = new AvroSchemaCompatibility.
                AvroIncompatibility("/fields/2", "baz", READER_FIELD_MISSING_DEFAULT_VALUE);

        Assert.notEmpty(incompatibilities, "should not be empty");
        Assertions.assertEquals(List.of(expected), incompatibilities);
    }

    @Test
    public void testRemoveOptionalFieldIsForwardCompatibleChange(){
        final var json = "{\"type\":[\"null\",\"string\"],\"name\":\"saz\",\"default\": null}";
        final var newBase = putAndGetJson(new JSONObject(json), BASE_RECORD_JSON);

        final var original = List.of(AvroUtils.getParsedSchema(newBase));
        final var newSchema = AvroUtils.getParsedSchema(
                removeAndGetJson("saz", newBase)
        );

        final var incompatibilities =
                avroSchemaCompatibility.validateSchema(original, newSchema, CompatibilityMode.FORWARD);

        Assert.isTrue(incompatibilities.isEmpty(), "should be empty");
    }

    @Test
    public void testAddOptionalFieldIsCompatibleChange(){

        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));

        final var json = "{\"type\":[\"null\",\"string\"],\"name\":\"saz\",\"default\": null}";
        final var newSchemaJson = putAndGetJson(new JSONObject(json), BASE_RECORD_JSON);
        final var newSchema = AvroUtils.getParsedSchema(newSchemaJson);

        final var incompatibilities =
                avroSchemaCompatibility.validateSchema(original, newSchema, CompatibilityMode.COMPATIBLE);

        Assert.isTrue(incompatibilities.isEmpty(), "should be empty");
    }

    @Test
    public void testRemoveOptionalFieldIsCompatibleChange(){
        final var json = "{\"type\":[\"null\",\"string\"],\"name\":\"saz\",\"default\": null}";
        final var newBase = putAndGetJson(new JSONObject(json), BASE_RECORD_JSON);

        final var original = List.of(AvroUtils.getParsedSchema(newBase));
        final var newSchema = AvroUtils.getParsedSchema(removeAndGetJson("saz", newBase));

        final var incompatibilities =
                avroSchemaCompatibility.validateSchema(original, newSchema, CompatibilityMode.COMPATIBLE);

        Assert.isTrue(incompatibilities.isEmpty(), "should be empty");
    }


    @Test
    public void testOrderOfFieldChangeIsAllowed(){
        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));

        final var orderChangedJson = putAndGetJson(
                new JSONObject("{\"type\":\"string\",\"name\":\"foo\"}"),
                removeAndGetJson("foo", BASE_RECORD_JSON)
        ); // now foo is at the end in new schema

        final var newSchema = AvroUtils.getParsedSchema(orderChangedJson);

        final var incompatibilities =
                Stream.of(CompatibilityMode.FORWARD, CompatibilityMode.COMPATIBLE).
                map(mode -> avroSchemaCompatibility.validateSchema(original, newSchema, mode)).
                flatMap(List::stream).collect(Collectors.toList());

        Assert.isTrue(incompatibilities.isEmpty(), "should be empty");
    }

    @Test
    public void testTypeChangeIsNotAllowed(){
        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));
        final var typeChangedJson = putAndGetJson(
                new JSONObject("{\"type\":\"string\",\"name\":\"baz\"}"),
                removeAndGetJson("baz", BASE_RECORD_JSON)
        ); // now baz went from int -> string
        final var newSchema = AvroUtils.getParsedSchema(typeChangedJson);

        final var incompatibilities =
                Stream.of(CompatibilityMode.FORWARD, CompatibilityMode.COMPATIBLE).
                map(mode -> avroSchemaCompatibility.validateSchema(original, newSchema, mode)).
                flatMap(List::stream).collect(Collectors.toList());

        Assert.notEmpty(incompatibilities, "should not be empty");

        final var incompatType =
                incompatibilities.stream().
                map(incompat -> incompat.getIncompatibilityType()).collect(Collectors.toSet());

        Assert.isTrue(incompatType.size() == 1);
        Assert.isTrue(incompatType.contains(TYPE_MISMATCH));
    }

    @Test
    public void testSchemaNameChangedNotAllowed(){
        final var original = List.of(AvroUtils.getParsedSchema(BASE_RECORD_JSON));
        final var typeChangedJson =
                BASE_RECORD_JSON.replace("NESTED_RECORD_SCHEMA_NAME", "NESTED_RECORD_SCHEMA_NAME_NEW");

        final var newSchema = AvroUtils.getParsedSchema(typeChangedJson);

        final var incompatibilities =
                Stream.of(CompatibilityMode.FORWARD, CompatibilityMode.COMPATIBLE).
                map(mode -> avroSchemaCompatibility.validateSchema(original, newSchema, mode)).
                flatMap(List::stream).collect(Collectors.toList());

        Assert.notEmpty(incompatibilities, "should not be empty");

        final var incompatType =
                incompatibilities.stream().
                        map(incompat -> incompat.getIncompatibilityType()).collect(Collectors.toSet());
        Assert.isTrue(incompatType.size() == 1);
        Assert.isTrue(incompatType.contains(NAME_MISMATCH));

    }

    @Test
    public void testAllCompatibilityModesAreSupported() {
        final var json = "{\"type\":[\"null\",\"string\"],\"name\":\"saz\",\"default\": null}";
        final var newBase = putAndGetJson(new JSONObject(json), BASE_RECORD_JSON);

        final var original = List.of(AvroUtils.getParsedSchema(newBase));
        final var newSchema = AvroUtils.getParsedSchema(removeAndGetJson("saz", newBase));

        //No exception is throw when below function is executed
        final var incompatibilities =
                Arrays.stream(CompatibilityMode.values()).
                map(mode -> avroSchemaCompatibility.validateSchema(original, newSchema, mode));
    }

    private String removeAndGetJson(final String elementName, final String schemaJson){
        final var baseSchema = new JSONObject(schemaJson);
        final var fields = baseSchema.getJSONArray("fields");

        for(int idx = 0; idx < fields.length(); idx++){
            final boolean found = fields.getJSONObject(idx).getString("name").equals(elementName);
            if(found){
                fields.remove(idx);
                break;
            }
        }
        return baseSchema.toString();
    }

    private String putAndGetJson(final JSONObject jsonObject, final String schemaJson){
        final JSONObject baseSchema = new JSONObject(schemaJson);
        baseSchema.getJSONArray("fields").put(jsonObject);
        return baseSchema.toString();
    }

    private static String getBaseRecordJson(){
        final var nestedRecordSchema = SchemaBuilder.record("NESTED_RECORD_SCHEMA_NAME").fields().
                name("foo").type().stringType().noDefault().
                name("bar").type().intType().noDefault().
                endRecord();

        return SchemaBuilder.
                        record("NAME_PLACE_HOLDER").fields().
                        name("foo").type().stringType().noDefault().
                        name("bar").type().stringType().noDefault().
                        name("baz").type().intType().noDefault().
                        name("NESTED_RECORD_NAME").type(nestedRecordSchema).noDefault().
                        endRecord().toString(true);
    }
}