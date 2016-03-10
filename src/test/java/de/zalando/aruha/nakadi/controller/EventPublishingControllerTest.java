package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryEventTypeRepository;
import de.zalando.aruha.nakadi.repository.InMemoryTopicRepository;
import de.zalando.aruha.nakadi.repository.db.EventTypeCache;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import de.zalando.aruha.nakadi.validation.EventTypeValidator;
import de.zalando.aruha.nakadi.validation.ValidationError;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import javax.ws.rs.core.Response;
import java.util.LinkedList;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

public class EventPublishingControllerTest {

    public static final String EVENT_TYPE_WITH_TOPIC = "my-topic";
    public static final String EVENT_TYPE_WITHOUT_TOPIC = "registered-but-without-topic";
    public static final String EVENT1 = "{\"payload\": \"My Event 1 Payload\"}";
    public static final String EVENT2 = "{\"payload\": \"My Event 2 Payload\"}";
    public static final String EVENT3 = "{\"payload\": \"My Event 3 Payload\"}";
    public static final String INVALID_SCHEMA_EVENT = "{\"wrong-payload\": \"My Event 3 Payload\"}";
    public static final String INVALID_JSON_EVENT = "not-a-json";
    public static final String[] PARTITIONS = new String[]{"0", "1", "2", "3", "4", "5", "6", "7"};

    private final EventTypeCache cache;
    private final EventTypeValidator validator;
    private final InMemoryTopicRepository topicRepository = new InMemoryTopicRepository();
    private final MetricRegistry metricRegistry;
    private final JsonTestHelper jsonHelper;

    private final MockMvc mockMvc;

    public EventPublishingControllerTest() throws NakadiException, ExecutionException {
        final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();

        jsonHelper = new JsonTestHelper(objectMapper);
        topicRepository.createTopic(EVENT_TYPE_WITH_TOPIC);

        final EventTypeRepository eventTypeRepository = new InMemoryEventTypeRepository();
        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITH_TOPIC));
        eventTypeRepository.saveEventType(eventType(EVENT_TYPE_WITHOUT_TOPIC));

        cache = mock(EventTypeCache.class);
        validator = mock(EventTypeValidator.class);

        Mockito
                .doReturn(validator)
                .when(cache)
                .getValidator(anyString());

        metricRegistry = new MetricRegistry();
        final EventPublishingController controller = new EventPublishingController(topicRepository, eventTypeRepository, cache, metricRegistry);

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter = new MappingJackson2HttpMessageConverter(objectMapper);
        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .build();
    }

    @Test
    public void canPostEventsToTopic() throws Exception {
        Mockito
                .doReturn(Optional.empty())
                .when(validator)
                .validate(any());

        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT1);
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT2);
        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT3);

        final LinkedList<String> events = topicRepository.getEvents(EVENT_TYPE_WITH_TOPIC, "0");

        assertThat(events, hasSize(3));

        assertThat(events.removeFirst(), equalTo(EVENT1));
        assertThat(events.removeFirst(), equalTo(EVENT2));
        assertThat(events.removeFirst(), equalTo(EVENT3));
    }

    @Test
    public void returns2xxForValidPost() throws Exception {
        Mockito
                .doReturn(Optional.empty())
                .when(validator)
                .validate(any());

        postEvent(EVENT_TYPE_WITH_TOPIC, EVENT1).andExpect(status().is2xxSuccessful());
    }

    @Test
    public void returns5xxProblemIfTopicDoesNotExistForEventType() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR, "No such topic 'registered-but-without-topic'");

        Mockito
                .doReturn(Optional.empty())
                .when(validator)
                .validate(any());

        postEvent(EVENT_TYPE_WITHOUT_TOPIC, EVENT1)
                .andExpect(status().is5xxServerError())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void returns404ProblemIfEventTypeIsNotRegistered() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(Response.Status.NOT_FOUND, "EventType 'does-not-exist' does not exist.");

        postEvent("does-not-exist", EVENT1)
                .andExpect(status().is4xxClientError())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void returns422ProblemWhenEventSchemaIsInvalid() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, "#: required key [payload] not found");

        Mockito
                .doReturn(Optional.of(new ValidationError("#: required key [payload] not found")))
                .when(validator)
                .validate(any());

        postEvent(EVENT_TYPE_WITH_TOPIC, INVALID_SCHEMA_EVENT)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void returns422ProblemWhenEventIsNotJson() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, "payload must be a valid json");

        postEvent(EVENT_TYPE_WITH_TOPIC, INVALID_JSON_EVENT)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void publishedEventsAreReportedPerEventType() throws Exception {
        when(validator.validate(Matchers.any(JSONObject.class))).then(invocation -> {
            final JSONObject jsonObject = (JSONObject) invocation.getArguments()[0];
            return jsonObject.has("fail") ? Optional.of(new ValidationError("Should fail")) : Optional.empty();
        });

        final Timer successfulTimer = metricRegistry.timer(
                metricNameFor(EVENT_TYPE_WITH_TOPIC, EventPublishingController.SUCCESS_METRIC_NAME));
        final Counter failedCounter = metricRegistry.counter(
                metricNameFor(EVENT_TYPE_WITH_TOPIC, EventPublishingController.FAILED_METRIC_NAME));

        assertThat(successfulTimer.getCount(), equalTo(0L));
        assertThat(failedCounter.getCount(), equalTo(0L));

        postEvent(EVENT_TYPE_WITH_TOPIC, "{}");
        postEvent(EVENT_TYPE_WITH_TOPIC, "{}");
        postEvent(EVENT_TYPE_WITH_TOPIC, "{}");

        postEvent(EVENT_TYPE_WITH_TOPIC, "{\"fail\": \"fail\"}");
        postEvent(EVENT_TYPE_WITH_TOPIC, "{\"fail\": \"fail\"}");

        assertThat(successfulTimer.getCount(), equalTo(3L));
        assertThat(failedCounter.getCount(), equalTo(2L));
    }

    private ResultActions postEvent(final String eventType, final String event) throws Exception {
        final String url = "/event-types/" + eventType + "/events";
        final MockHttpServletRequestBuilder requestBuilder = post(url)
                .contentType(APPLICATION_JSON)
                .content(event);

        return mockMvc.perform(requestBuilder);
    }

    private static EventType eventType(final String topic) {
        final EventType eventType = new EventType();
        final EventTypeSchema schema = new EventTypeSchema();
        schema.setSchema("{\"type\": \"object\", \"properties\": {\"payload\": {\"type\": \"string\"}}, \"required\": [\"payload\"]}");
        schema.setType(EventTypeSchema.Type.JSON_SCHEMA);
        eventType.setName(topic);
        eventType.setSchema(schema);
        return eventType;
    }

}