package de.zalando.aruha.nakadi.utils;

import java.io.IOException;

import java.util.Random;
import java.util.UUID;

import org.apache.commons.io.IOUtils;

import org.json.JSONObject;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;

public class TestUtils {

    private static final String VALID_EVENT_TYPE_NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMOPQRSTUVWXYZ";
    private static final String VALID_EVENT_BODY_CHARS = VALID_EVENT_TYPE_NAME_CHARS + " \t!@#$%^&*()=+-_";

    private static final Random RANDOM = new Random();

    private TestUtils() { }

    public static String randomUUID() {
        return UUID.randomUUID().toString();
    }

    public static String randomTextString() {
        return randomString(VALID_EVENT_TYPE_NAME_CHARS);
    }

    public static String randomString() {
        final int length = RANDOM.nextInt(500);

        String s = "";

        for (int i = 0; i < length; i++) {
            s += (char) RANDOM.nextInt(128);
        }

        return s;

    }

    public static String randomString(final String validChars) {
        final int length = RANDOM.nextInt(49) + 1;

        String s = "";

        for (int i = 0; i < length; i++) {
            s += validChars.charAt(RANDOM.nextInt(validChars.length()));
        }

        return s;

    }

    public static String randomValidEventTypeName() {
        return String.format("%s.%s", randomString(VALID_EVENT_TYPE_NAME_CHARS),
                randomString(VALID_EVENT_TYPE_NAME_CHARS));
    }

    public static int randomUInt() {
        return RANDOM.nextInt(Integer.MAX_VALUE);
    }

    public static String randomUIntAsString() {
        return Integer.toString(randomUInt());
    }

    public static long randomULong() {
        return randomUInt() * randomUInt();
    }

    public static String randomULongAsString() {
        return Long.toString(randomULong());
    }

    public static String resourceAsString(final String resourceName, final Class clazz) throws IOException {
        return IOUtils.toString(clazz.getResourceAsStream(resourceName));
    }

    public static EventType buildEventType(final String name, final JSONObject schema) {
        final EventType et = new EventType();
        et.setName(name);

        final EventTypeSchema ets = new EventTypeSchema();
        ets.setType(EventTypeSchema.Type.JSON_SCHEMA);
        ets.setSchema(schema.toString());
        et.setSchema(ets);
        et.setCategory(EventCategory.UNDEFINED);
        et.setOwningApplication("event-producer-application");

        return et;
    }

    public static EventType buildDefaultEventType() {
        return buildEventType(randomValidEventTypeName(), new JSONObject("{ \"price\": 1000 }"));
    }
}
