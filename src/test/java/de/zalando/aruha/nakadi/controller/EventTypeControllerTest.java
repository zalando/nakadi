package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventCategory;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.EventTypeSchema;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.UnprocessableEntityException;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicCreationException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.json.JSONObject;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.validation.Errors;
import org.springframework.validation.FieldError;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import javax.ws.rs.core.Response;
import java.util.Arrays;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventTypeControllerTest {

    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final TopicRepository topicRepository = mock(TopicRepository.class);

    public static final String EVENT_TYPE_NAME = "event-name";

    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final MockMvc mockMvc;

    public EventTypeControllerTest() throws Exception {
        EventTypeController controller = new EventTypeController(eventTypeRepository, topicRepository);

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
                new MappingJackson2HttpMessageConverter(objectMapper);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .build();
    }

    @Test
    public void whenPostWithInvalidEventTypeThenReturn422() throws Exception {
        EventType invalidEventType = buildEventType();
        invalidEventType.setName("");

        Problem expectedProblem = invalidProblem("name", "may not be empty");

        postEventType(invalidEventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostWithNoCategoryThenReturn422() throws Exception {
        EventType invalidEventType = buildEventType();
        final JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(invalidEventType));

        jsonObject.remove("category");

        Problem expectedProblem = invalidProblem("category", "may not be null");

        postEventType(jsonObject.toString())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostDuplicatedEventTypeReturn409() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.CONFLICT, "some-name");

        Mockito.
                doThrow(new DuplicatedEventTypeNameException("some-name")).
                when(eventTypeRepository).
                saveEventType(any(EventType.class));

        postEventType(buildEventType())
                .andExpect(status().isConflict())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPersistencyErrorThen500() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);

        Mockito
                .doThrow(InternalNakadiException.class)
                .when(eventTypeRepository)
                .saveEventType(any(EventType.class));

        postEventType(buildEventType())
                .andExpect(status().isInternalServerError())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenCreateSuccessfullyThen201() throws Exception {
        Mockito
                .doNothing()
                .when(eventTypeRepository)
                .saveEventType(any(EventType.class));

        Mockito
                .doNothing()
                .when(topicRepository)
                .createTopic("event-name");

        postEventType(buildEventType())
                .andExpect(status().isCreated())
                .andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic("event-name");
    }

    @Test
    public void whenTopicCreationFailsRemoveEventTypeFromRepositoryAnd500() throws Exception {
        Mockito
                .doNothing()
                .when(eventTypeRepository)
                .saveEventType(any(EventType.class));

        Mockito
                .doThrow(TopicCreationException.class)
                .when(topicRepository)
                .createTopic("event-name");

        Mockito
                .doNothing()
                .when(eventTypeRepository)
                .removeEventType("event-name");

        final Problem expectedProblem = Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE);

        postEventType(buildEventType())
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic("event-name");
        verify(eventTypeRepository, times(1)).removeEventType("event-name");
    }

    @Test
    public void whenPUTInvalidEventTypeThen422() throws Exception {
        EventType invalidEventType = buildEventType();
        final JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(invalidEventType));

        jsonObject.remove("category");

        Problem expectedProblem = invalidProblem("category", "may not be null");

        putEventType(jsonObject.toString(), invalidEventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTDifferentEventTypeNameThen422() throws Exception {
        EventType eventType = buildEventType();
        eventType.setName("event-name-different");

        Problem expectedProblem = invalidProblem("name",
                "The submitted event type name \"event-name-different\" should match the parameter name \"event-name\"");

        Mockito
                .doReturn(eventType)
                .when(eventTypeRepository)
                .findByName(EVENT_TYPE_NAME);

        putEventType(eventType, EVENT_TYPE_NAME)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTDifferentEventTypeSchemaThen422() throws Exception {
        EventType eventType = buildEventType();
        EventType persistedEventType = buildEventType();
        persistedEventType.getSchema().setSchema("different");

        Problem expectedProblem = invalidProblem("schema",
                "The schema you've just submitted is different from the one in our system.");

        Mockito
                .doReturn(persistedEventType)
                .when(eventTypeRepository)
                .findByName(EVENT_TYPE_NAME);

        putEventType(eventType, EVENT_TYPE_NAME)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTInexistingEventTypeThen404() throws Exception {
        EventType eventType = buildEventType();

        Problem expectedProblem = Problem.valueOf(NOT_FOUND);

        Mockito
                .doThrow(NoSuchEventTypeException.class)
                .when(eventTypeRepository)
                .findByName(EVENT_TYPE_NAME);

        putEventType(eventType, EVENT_TYPE_NAME)
                .andExpect(status().isNotFound())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTRepoNakadiExceptionThen422() throws Exception {
        EventType eventType = buildEventType();

        Problem expectedProblem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY);

        Mockito
                .doThrow(UnprocessableEntityException.class)
                .when(eventTypeRepository)
                .findByName(EVENT_TYPE_NAME);

        putEventType(eventType, EVENT_TYPE_NAME)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }
    
    @Test
    public void canExposeASingleEventType() throws Exception {
        final EventType expectedEventType = buildEventType();

        when(eventTypeRepository.findByName(EVENT_TYPE_NAME)).thenReturn(expectedEventType);

        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + EVENT_TYPE_NAME)
                .accept(APPLICATION_JSON);

        mockMvc.perform(requestBuilder)
                .andExpect(status().is(200))
                .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON))
                .andExpect(content().json(asJsonString(expectedEventType)));

    }

    @Test
    public void askingForANonExistingEventTypeResultsIn404() throws Exception {
        when(eventTypeRepository.findByName(anyString())).thenThrow(new NoSuchEventTypeException("EventType 'event-name' does not exist."));

        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + EVENT_TYPE_NAME)
                .accept(APPLICATION_JSON);

        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND,
                "EventType '" + EVENT_TYPE_NAME + "' does not exist.");

        mockMvc.perform(requestBuilder)
                .andExpect(status().is(404))
                .andExpect(content().contentTypeCompatibleWith("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));

    }

    @Test
    public void whenEventTypeSchemaJsonIsMalformedThen422() throws Exception {
        EventType eventType = buildEventType();
        eventType.getSchema().setSchema("invalid-json");

        Problem expectedProblem = invalidProblem("schema.schema", "must be a valid json");

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect((content().string(matchesProblem(expectedProblem))));
    }

    @Test
    public void invalidEventTypeSchemaJsonSchemaThen422() throws Exception {
        EventType eventType = buildEventType();

        final String jsonSchemaString = Resources.toString(Resources.getResource("sample-invalid-json-schema.json"), Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        Problem expectedProblem = invalidProblem("schema.schema", "must be valid json-schema (http://json-schema.org)");

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect((content().string(matchesProblem(expectedProblem))));
    }

    private ResultActions postEventType(EventType eventType) throws Exception {
        String content = objectMapper.writeValueAsString(eventType);

        return postEventType(content);
    }

    private ResultActions postEventType(final String content) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/event-types")
                .contentType(APPLICATION_JSON)
                .content(content);

        return mockMvc.perform(requestBuilder);
    }

    private ResultActions putEventType(EventType eventType, String name) throws Exception {
        String content = objectMapper.writeValueAsString(eventType);

        return putEventType(content, name);
    }

    private ResultActions putEventType(final String content, final String name) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name)
                .contentType(APPLICATION_JSON)
                .content(content);

        return mockMvc.perform(requestBuilder);
    }

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

    private Problem invalidProblem(String field, String description) {
        FieldError[] fieldErrors = { new FieldError("", field, description) };

        Errors errors = mock(Errors.class);
        when(errors.getAllErrors()).thenReturn(Arrays.asList(fieldErrors));
        return new ValidationProblem(errors);
    }

    private SameJSONAs<? super String> matchesProblem(final Problem expectedProblem) throws JsonProcessingException {
        return sameJSONAs(asJsonString(expectedProblem));
    }

    private String asJsonString(final Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }
}
