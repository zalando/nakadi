package de.zalando.aruha.nakadi.utils;

import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;

import com.fasterxml.jackson.core.JsonProcessingException;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class TestUtils {


    private static final String VALID_EVENT_TYPE_NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMOPQRSTUVWXYZ";



    private static final Random RANDOM = new Random();

    private TestUtils() { }

    public static String randomUUID() {
        return UUID.randomUUID().toString();
    }

    public static String randomString(final String validChars) {
    	final int length = RANDOM.nextInt(50);

    	String s = "";

    	for (int i = 0; i < length; i++) {
			s += validChars.charAt(RANDOM.nextInt(validChars.length()));
		}

    	return s;

    }

    public static String randomValidEventTypeName() {
    	return String.format("%s.%s", randomString(VALID_EVENT_TYPE_NAME_CHARS), randomString(VALID_EVENT_TYPE_NAME_CHARS));
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

    public static EventType buildEventType(final String name, final String schema) {
        final EventType et = new EventType();
        et.setName(name);

        final EventTypeSchema ets = new EventTypeSchema();
        ets.setType(EventTypeSchema.Type.JSON_SCHEMA);
        ets.setSchema(schema);
        et.setSchema(ets);
        et.setCategory(EventCategory.UNDEFINED);
        et.setOwningApplication("event-producer-application");

        return et;
    }


    public static EventType buildDefaultEventType () {
    	return buildEventType(randomValidEventTypeName(), "{ \"price\": 1000 }");
    }
/*
    private EventType buildEventType() throws JsonProcessingException {

        final EventTypeSchema schema = new EventTypeSchema();
        final EventType eventType = new EventType();

        schema.setSchema("{ \"price\": 1000 }");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);

        eventType.setName(EVENT_TYPE_NAME);
        eventType.setCategory(EventCategory.UNDEFINED);
        eventType.setSchema(schema);
        eventType.setOwningApplication("event-producer-application");

        return eventType;
    }
*/
}
