package org.zalando.nakadi.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.json.JSONObject;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.UUID;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

public class TestUtils {

    private static final String VALID_EVENT_TYPE_NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMOPQRSTUVWXYZ";

    private static final Random RANDOM = new Random();
    private static final ObjectMapper OBJECT_MAPPER = new JsonConfig().jacksonObjectMapper();

    public static String randomUUID() {
        return UUID.randomUUID().toString();
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

    public static long randomULong() {
        return randomUInt() * randomUInt();
    }

    public static String resourceAsString(final String resourceName, final Class clazz) throws IOException {
        return IOUtils.toString(clazz.getResourceAsStream(resourceName));
    }

    public static EventType buildDefaultEventType() {
        return EventTypeTestBuilder.builder().build();
    }

    public static String readFile(final String filename) throws IOException {
        return Resources.toString(Resources.getResource(filename), Charsets.UTF_8);
    }

    public static JSONObject buildBusinessEvent() throws IOException {
        final String json = Resources.toString(Resources.getResource("sample-business-event.json"), Charsets.UTF_8);
        return new JSONObject(json);
    }

    public static EventType loadEventType(final String filename) throws IOException {
        final String json = readFile(filename);
        return OBJECT_MAPPER.readValue(json, EventType.class);
    }

    public static MappingJackson2HttpMessageConverter createMessageConverter() {
        return new MappingJackson2HttpMessageConverter(new JsonConfig().jacksonObjectMapper());
    }

    public static MockMvc mockMvcForController(final Object controller) {
        return standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), createMessageConverter())
                .build();
    }

    public static Problem invalidProblem(final String field, final String description) {
        final FieldError[] fieldErrors = {new FieldError("", field, description)};

        final Errors errors = mock(Errors.class);
        when(errors.getAllErrors()).thenReturn(Arrays.asList(fieldErrors));
        return new ValidationProblem(errors);
    }

    public static BatchItem createBatch(final JSONObject event) {
        return new BatchItem(event);
    }

}
