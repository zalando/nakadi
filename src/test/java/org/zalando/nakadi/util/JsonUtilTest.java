package org.zalando.nakadi.util;

import org.junit.Assert;
import org.junit.Test;

public class JsonUtilTest {

    @Test
    public void testValidateSchemaEndingBracket() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"additionalProperties\": true}}"));
    }

    @Test
    public void testValidateSchemaMultipleRoots() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"additionalProperties\": true}{\"additionalProperties\": true}"));
    }

    @Test
    public void testValidateSchemaArbitraryEnding() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"additionalProperties\": true}NakadiRocks"));
    }

    @Test
    public void testValidateSchemaArrayEnding() {
        Assert.assertFalse(JsonUtil.isValidJson("[{\"additionalProperties\": true}]]"));
    }

    @Test
    public void testValidateSchemaEndingSpaces() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"additionalProperties\": true}   "));
    }

    @Test
    public void testValidateSchemaEndingNewlines() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"additionalProperties\": true}\n\r\t"));
    }

    @Test
    public void testValidateSchemaEndingCommaArray() {
        Assert.assertFalse(JsonUtil.isValidJson("[{\"test\": true},]"));
    }

    @Test
    public void testValidateSchemaEndingCommaArray2() {
        Assert.assertFalse(JsonUtil.isValidJson("[\"test\",]"));
    }

    @Test
    public void testValidateSchemaEndingCommaObject() {
        Assert.assertFalse(JsonUtil.isValidJson("{\"test\": true,}"));
    }

    @Test
    public void testValidateSchemaFormattedJson() {
        Assert.assertTrue(JsonUtil.isValidJson("{\n" +
                "properties:{\n" +
                "event:{\n" +
                "type:\"string\"\n" +
                "},\n" +
                "topic:{\n" +
                "type:\"string\"\n" +
                "},\n" +
                "meta_data:{\n" +
                "$ref:\"#/definitions/metadata\"\n" +
                "},\n" +
                "body:{\n" +
                "$ref:\"#/definitions/tote_state\"\n" +
                "}\n" +
                "},\n" +
                "definitions:{\n" +
                "metadata:{\n" +
                "properties:{\n" +
                "occured_at:{\n" +
                "type:\"string\"\n" +
                "},\n" +
                "eid:{\n" +
                "type:\"string\"\n" +
                "}\n" +
                "},\n" +
                "type:\"object\"\n" +
                "},\n" +
                "tote_state:{\n" +
                "properties:{\n" +
                "barcode:{\n" +
                "type:\"string\"\n" +
                "},\n" +
                "state:{\n" +
                "enum:[\n" +
                "\"/status/closed\"\n" +
                "]\n" +
                "}\n" +
                "},\n" +
                "type:\"object\"\n" +
                "}\n" +
                "},\n" +
                "type:\"object\"\n" +
                "}"));
    }

}