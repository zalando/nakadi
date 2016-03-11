package de.zalando.aruha.nakadi.controller;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import static org.springframework.http.MediaType.APPLICATION_JSON;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.delete;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

import static de.zalando.aruha.nakadi.domain.EventCategory.BUSINESS;
import static de.zalando.aruha.nakadi.utils.TestUtils.buildDefaultEventType;

import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

import java.util.Arrays;

import javax.ws.rs.core.Response;

import de.zalando.aruha.nakadi.exceptions.InvalidEventTypeException;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import com.google.common.base.Charsets;
import com.google.common.io.Resources;

import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.exceptions.DuplicatedEventTypeNameException;
import de.zalando.aruha.nakadi.exceptions.InternalNakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.TopicCreationException;
import de.zalando.aruha.nakadi.exceptions.TopicDeletionException;
import de.zalando.aruha.nakadi.exceptions.UnprocessableEntityException;
import de.zalando.aruha.nakadi.problem.ValidationProblem;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.utils.TestUtils;

import uk.co.datumedge.hamcrest.json.SameJSONAs;

public class EventTypeControllerTest {

    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final TopicRepository topicRepository = mock(TopicRepository.class);

    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final MockMvc mockMvc;

    public EventTypeControllerTest() throws Exception {
        final EventTypeController controller = new EventTypeController(eventTypeRepository, topicRepository);

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
            new MappingJackson2HttpMessageConverter(objectMapper);

        mockMvc = standaloneSetup(controller).setMessageConverters(new StringHttpMessageConverter(),
                jackson2HttpMessageConverter).build();
    }

    @Test
    public void eventTypeWithEmptyNameReturns422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        invalidEventType.setName("?");

        final Problem expectedProblem = invalidProblem("name", "format not allowed");

        postEventType(invalidEventType).andExpect(status().isUnprocessableEntity())
                                       .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                               .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostWithEventTypeNameNotSetThenReturn422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        invalidEventType.setName(null);

        final Problem expectedProblem = invalidProblem("name", "may not be null");

        postEventType(invalidEventType).andExpect(status().isUnprocessableEntity())
                                       .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                               .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostWithNoCategoryThenReturn422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        final JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(invalidEventType));

        jsonObject.remove("category");

        final Problem expectedProblem = invalidProblem("category", "may not be null");

        postEventType(jsonObject.toString()).andExpect(status().isUnprocessableEntity())
                                            .andExpect(content().contentType("application/problem+json")).andExpect(
                                                content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostWithNoSchemaSchemaThenReturn422() throws Exception {
        final Problem expectedProblem = invalidProblem("schema.schema", "may not be null");

        final String eventType = "{\"category\": \"data\", \"owning_application\": \"blah-app\", "
                + "\"name\": \"blah-event-type\", \"schema\": { \"type\": \"JSON_SCHEMA\" }}";

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                                .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                        .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPOSTBusinessEventTypeMetadataThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getSchema().setSchema(
            "{\"type\": \"object\", \"properties\": {\"metadata\": {\"type\": \"object\"} }}");
        eventType.setCategory(BUSINESS);

        final Problem expectedProblem = new InvalidEventTypeException("\"metadata\" property is reserved").asProblem();

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                                .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                        .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostDuplicatedEventTypeReturn409() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.CONFLICT, "some-name");

        Mockito.doThrow(new DuplicatedEventTypeNameException("some-name")).when(eventTypeRepository).saveEventType(any(
                EventType.class));

        postEventType(buildDefaultEventType()).andExpect(status().isConflict())
                                              .andExpect(content().contentType("application/problem+json")).andExpect(
                                                  content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostAndTopicExistsReturn409() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.CONFLICT, "dummy message");
        final EventType et = buildDefaultEventType();
        Mockito.doNothing().when(eventTypeRepository).saveEventType(any(EventType.class));

        Mockito.doThrow(new DuplicatedEventTypeNameException("dummy message")).when(topicRepository).createTopic(
            et.getName());

        postEventType(et).andExpect(status().isConflict()).andExpect(content().contentType("application/problem+json"))
                         .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeThenOk() throws Exception {

        final String eventTypeName = TestUtils.randomValidEventTypeName();

        Mockito.doNothing().when(eventTypeRepository).removeEventType(eventTypeName);

        Mockito.doNothing().when(topicRepository).deleteTopic(eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isOk()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).removeEventType(eventTypeName);
        verify(topicRepository, times(1)).deleteTopic(eventTypeName);
    }

    @Test
    public void whenDeleteNoneExistingEventTypeThen404() throws Exception {

        final String eventTypeName = TestUtils.randomValidEventTypeName();
        final Problem expectedProblem = Problem.valueOf(Response.Status.NOT_FOUND, "dummy message");

        Mockito.doThrow(new NoSuchEventTypeException("dummy message")).when(eventTypeRepository).removeEventType(
            eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isNotFound())
                                      .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                              .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeAndTopicDeletionExceptionThen503() throws Exception {

        final String eventTypeName = TestUtils.randomValidEventTypeName();
        final Problem expectedProblem = Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE, "dummy message");

        Mockito.doThrow(new TopicDeletionException("dummy message", null)).when(topicRepository).deleteTopic(
            eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isServiceUnavailable())
                                      .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                              .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeAndNakadiExceptionThen500() throws Exception {

        final String eventTypeName = TestUtils.randomValidEventTypeName();
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR, "dummy message");

        Mockito.doThrow(new InternalNakadiException("dummy message")).when(eventTypeRepository).removeEventType(
            eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isInternalServerError())
                                      .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                              .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPersistencyErrorThen500() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);

        Mockito.doThrow(InternalNakadiException.class).when(eventTypeRepository).saveEventType(any(EventType.class));

        postEventType(buildDefaultEventType()).andExpect(status().isInternalServerError())
                                              .andExpect(content().contentType("application/problem+json")).andExpect(
                                                  content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenCreateSuccessfullyThen201() throws Exception {
        final EventType et = buildDefaultEventType();

        Mockito.doNothing().when(eventTypeRepository).saveEventType(any(EventType.class));

        Mockito.doNothing().when(topicRepository).createTopic(et.getName());

        postEventType(et).andExpect(status().isCreated()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(et.getName());
    }

    @Test
    public void whenTopicCreationFailsRemoveEventTypeFromRepositoryAnd500() throws Exception {

        final EventType et = buildDefaultEventType();
        Mockito.doNothing().when(eventTypeRepository).saveEventType(any(EventType.class));

        Mockito.doThrow(TopicCreationException.class).when(topicRepository).createTopic(et.getName());

        Mockito.doNothing().when(eventTypeRepository).removeEventType(et.getName());

        final Problem expectedProblem = Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE);

        postEventType(et).andExpect(status().isServiceUnavailable())
                         .andExpect(content().contentType("application/problem+json")).andExpect(content().string(
                                 matchesProblem(expectedProblem)));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(et.getName());
        verify(eventTypeRepository, times(1)).removeEventType(et.getName());
    }

    @Test
    public void whenPUTInvalidEventTypeThen422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        final JSONObject jsonObject = new JSONObject(objectMapper.writeValueAsString(invalidEventType));

        jsonObject.remove("category");

        final Problem expectedProblem = invalidProblem("category", "may not be null");

        putEventType(jsonObject.toString(), invalidEventType.getName()).andExpect(status().isUnprocessableEntity())
                                                                       .andExpect(content().contentType(
                                                                               "application/problem+json")).andExpect(
                                                                           content().string(
                                                                               matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTDifferentEventTypeNameThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final String eventTypeName = eventType.getName();
        eventType.setName("event-name-different");

        final Problem expectedProblem = new InvalidEventTypeException("path does not match resource name").asProblem();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventTypeName);

        putEventType(eventType, eventTypeName).andExpect(status().isUnprocessableEntity())
                                              .andExpect(content().contentType("application/problem+json")).andExpect(
                                                  content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTDifferentEventTypeSchemaThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        final EventType persistedEventType = buildDefaultEventType();
        persistedEventType.setName(eventType.getName());
        persistedEventType.getSchema().setSchema("different");

        final Problem expectedProblem = new InvalidEventTypeException("schema must not be changed").asProblem();

        Mockito.doReturn(persistedEventType).when(eventTypeRepository).findByName(persistedEventType.getName());

        putEventType(eventType, persistedEventType.getName()).andExpect(status().isUnprocessableEntity())
                                                             .andExpect(content().contentType(
                                                                     "application/problem+json")).andExpect(content()
                                                                     .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTInexistingEventTypeThen404() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(NOT_FOUND);

        Mockito.doThrow(NoSuchEventTypeException.class).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isNotFound())
                                                    .andExpect(content().contentType("application/problem+json"))
                                                    .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTRepoNakadiExceptionThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY);

        Mockito.doThrow(UnprocessableEntityException.class).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isUnprocessableEntity())
                                                    .andExpect(content().contentType("application/problem+json"))
                                                    .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void canExposeASingleEventType() throws Exception {
        final EventType expectedEventType = buildDefaultEventType();

        when(eventTypeRepository.findByName(expectedEventType.getName())).thenReturn(expectedEventType);

        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + expectedEventType.getName()).accept(
                APPLICATION_JSON);

        mockMvc.perform(requestBuilder).andExpect(status().is(200))
               .andExpect(content().contentTypeCompatibleWith(APPLICATION_JSON)).andExpect(content().json(
                       asJsonString(expectedEventType)));

    }

    @Test
    public void askingForANonExistingEventTypeResultsIn404() throws Exception {
        final String eventTypeName = TestUtils.randomValidEventTypeName();
        when(eventTypeRepository.findByName(anyString())).thenThrow(new NoSuchEventTypeException(
                String.format("EventType '%s' does not exist.", eventTypeName)));

        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + eventTypeName).accept(
                APPLICATION_JSON);

        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND,
                "EventType '" + eventTypeName + "' does not exist.");

        mockMvc.perform(requestBuilder).andExpect(status().is(404))
               .andExpect(content().contentTypeCompatibleWith("application/problem+json")).andExpect(content().string(
                       matchesProblem(expectedProblem)));

    }

    @Test
    public void whenEventTypeSchemaJsonIsMalformedThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getSchema().setSchema("invalid-json");

        final Problem expectedProblem = new InvalidEventTypeException("schema must be a valid json").asProblem();

        postEventType(eventType).andExpect(status().isUnprocessableEntity()).andExpect((content().string(
                    matchesProblem(expectedProblem))));
    }

    @Test
    public void invalidEventTypeSchemaJsonSchemaThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final String jsonSchemaString = Resources.toString(Resources.getResource("sample-invalid-json-schema.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        final Problem expectedProblem = new InvalidEventTypeException("schema must be a valid json-schema").asProblem();

        postEventType(eventType).andExpect(status().isUnprocessableEntity()).andExpect((content().string(
                    matchesProblem(expectedProblem))));
    }

    private ResultActions deleteEventType(final String eventTypeName) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName));
    }

    private ResultActions postEventType(final EventType eventType) throws Exception {
        final String content = objectMapper.writeValueAsString(eventType);

        return postEventType(content);
    }

    private ResultActions postEventType(final String content) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = post("/event-types").contentType(APPLICATION_JSON).content(
                content);

        return mockMvc.perform(requestBuilder);
    }

    private ResultActions putEventType(final EventType eventType, final String name) throws Exception {
        final String content = objectMapper.writeValueAsString(eventType);

        return putEventType(content, name);
    }

    private ResultActions putEventType(final String content, final String name) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name).contentType(APPLICATION_JSON)
                                                                                        .content(content);

        return mockMvc.perform(requestBuilder);
    }

    private Problem invalidProblem(final String field, final String description) {
        final FieldError[] fieldErrors = {new FieldError("", field, description)};

        final Errors errors = mock(Errors.class);
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
