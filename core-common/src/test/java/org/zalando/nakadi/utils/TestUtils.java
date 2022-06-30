package org.zalando.nakadi.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import org.apache.commons.io.IOUtils;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.core.io.DefaultResourceLoader;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.BatchFactory;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.mapper.NakadiRecordMapper;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.problem.ValidationProblem;
import org.zalando.nakadi.service.LocalSchemaRegistry;
import org.zalando.problem.Problem;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toList;
import static java.util.stream.IntStream.range;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class TestUtils {

    private static final String VALID_EVENT_TYPE_NAME_CHARS = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMOPQRSTUVWXYZ";

    private static final Random RANDOM = new Random();
    public static final ObjectMapper OBJECT_MAPPER = new JsonConfig().jacksonObjectMapper();
    public static final JsonTestHelper JSON_TEST_HELPER = new JsonTestHelper(OBJECT_MAPPER);
    public static final MappingJackson2HttpMessageConverter JACKSON_2_HTTP_MESSAGE_CONVERTER =
            new MappingJackson2HttpMessageConverter(OBJECT_MAPPER);

    public static String randomUUID() {
        return UUID.randomUUID().toString();
    }

    public static String randomTextString() {
        return randomString(VALID_EVENT_TYPE_NAME_CHARS);
    }

    public static String randomString() {
        final int length = RANDOM.nextInt(100);

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

    public static String randomStringOfLength(final int length) {
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < length; i++) {
            sb.append((char) RANDOM.nextInt(128));
        }

        return sb.toString();
    }

    public static String randomValidStringOfLength(final int length) {
        final StringBuilder sb = new StringBuilder();

        for (int i = 0; i < length; i++) {
            sb.append(VALID_EVENT_TYPE_NAME_CHARS.charAt(RANDOM.nextInt(VALID_EVENT_TYPE_NAME_CHARS.length())));
        }

        return sb.toString();
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

    public static String resourceAsString(final String resourceName, final Class clazz) throws IOException {
        return IOUtils.toString(clazz.getResourceAsStream(resourceName));
    }

    public static EventType buildDefaultEventType() {
        return EventTypeTestBuilder.builder().build();
    }

    public static ResourceAuthorization buildResourceAuthorization() {
        final List<AuthorizationAttribute> admins = new ArrayList<>();
        final List<AuthorizationAttribute> readers = new ArrayList<>();
        final List<AuthorizationAttribute> writers = new ArrayList<>();
        return new ResourceAuthorization(admins, readers, writers);
    }

    public static AccessDeniedException mockAccessDeniedException() {
        final Resource resource = mock(Resource.class);
        when(resource.getName()).thenReturn("some-name");
        when(resource.getType()).thenReturn("some-type");
        return new AccessDeniedException(AuthorizationService.Operation.READ, resource);
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

    public static MockMvc mockMvcForController(final Object controller) {
        return standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .build();
    }

    public static Problem invalidProblem(final String field, final String description) {
        final FieldError[] fieldErrors = {new FieldError("", field, description)};

        final Errors errors = mock(Errors.class);
        when(errors.getAllErrors()).thenReturn(Arrays.asList(fieldErrors));
        return new ValidationProblem(errors);
    }

    public static Problem createInvalidEventTypeExceptionProblem(final String message) {
        return Problem.valueOf(UNPROCESSABLE_ENTITY, message);
    }

    public static void waitFor(final Runnable runnable) {
        waitFor(runnable, 10000, 500);
    }

    public static void waitFor(final Runnable runnable, final long timeoutMs) {
        waitFor(runnable, timeoutMs, 500);
    }

    @SuppressWarnings("unchecked")
    public static void waitFor(final Runnable runnable, final long timeoutMs, final int intervalMs,
                               final Class<? extends Throwable>... additionalException) {
        final List<Class<? extends Throwable>> leadToRetry =
                Stream.concat(Stream.of(additionalException), Stream.of(AssertionError.class)).collect(toList());
        executeWithRetry(
                runnable,
                new RetryForSpecifiedTimeStrategy<Void>(timeoutMs)
                        .withExceptionsThatForceRetry(leadToRetry)
                        .withWaitBetweenEachTry(intervalMs));
    }

    public static BatchItem createBatchItem(final JSONObject event) {
        return BatchFactory.from(new JSONArray().put(event).toString()).get(0);
    }

    public static BatchItem createBatchItem(final String event) {
        return BatchFactory.from("[" + event + "]").get(0);
    }

    public static DateTime randomDate() {
        final long maxMillis = new DateTime().getMillis();
        final long randomMillis = Math.round(Math.random() * maxMillis);
        return new DateTime(randomMillis, DateTimeZone.UTC);
    }

    public static List<Subscription> createRandomSubscriptions(final int count, final String owningApp) {
        return range(0, count)
                .mapToObj(i -> RandomSubscriptionBuilder.builder().withOwningApplication(owningApp).build())
                .collect(toList());
    }

    public static List<Subscription> createRandomSubscriptions(final int count) {
        return createRandomSubscriptions(count, randomTextString());
    }

    public static Subscription createSubscription(final String owningApp, final String consumerGroup) {
        return RandomSubscriptionBuilder.builder()
                .withConsumerGroup(consumerGroup)
                .withOwningApplication(owningApp)
                .build();
    }

    public static Timeline buildTimeline(final String etName) {
        return new Timeline(etName, 0, new Storage("ccc", Storage.Type.KAFKA), randomUUID(), new Date());
    }

    public static Timeline buildTimeline(final String etName, final String topic, final Date createdAt) {
        return new Timeline(etName, 0, new Storage("ccc", Storage.Type.KAFKA), topic, createdAt);
    }

    public static Timeline buildTimelineWithTopic(final String topic) {
        return new Timeline(randomUUID(), 0, new Storage("ccc", Storage.Type.KAFKA), topic, new Date());
    }

    public static String toTimelineOffset(final long offset) {
        return String.format("001-0001-%018d", offset);
    }

    public static NakadiRecordMapper getNakadiRecordMapper() throws IOException {
        return new NakadiRecordMapper(getLocalSchemaRegistry());
    }

    public static LocalSchemaRegistry getLocalSchemaRegistry() throws IOException {
        return new LocalSchemaRegistry(new DefaultResourceLoader().getResource("avro-schema/"));
    }

}
