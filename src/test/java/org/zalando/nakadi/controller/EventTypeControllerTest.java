package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.io.Resources;
import com.sun.security.auth.UserPrincipal;
import org.hamcrest.core.StringContains;
import org.json.JSONObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.ResultActions;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeStatistics;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.enrichment.Enrichment;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.UnprocessableEntityException;
import org.zalando.nakadi.partitioning.PartitionResolver;
import org.zalando.nakadi.plugin.api.ApplicationService;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.EventTypeService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.util.UUIDGenerator;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.validation.EventTypeOptionsValidator;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import uk.co.datumedge.hamcrest.json.SameJSONAs;

import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.Optional;
import java.util.UUID;

import static javax.ws.rs.core.Response.Status.FORBIDDEN;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doReturn;
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
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_APPLICATION_LEVEL_PERMISSIONS;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.CHECK_PARTITIONS_KEYS;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.invalidProblem;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONAs;

public class EventTypeControllerTest {

    private static final long TOPIC_RETENTION_MIN_MS = 100;
    private static final long TOPIC_RETENTION_MAX_MS = 200;
    private static final long TOPIC_RETENTION_TIME_MS = 150;
    private static final int NAKADI_SEND_TIMEOUT = 10000;
    private static final int NAKADI_POLL_TIMEOUT = 10000;
    private final EventTypeRepository eventTypeRepository = mock(EventTypeRepository.class);
    private final TopicRepository topicRepository = mock(TopicRepository.class);
    private final PartitionResolver partitionResolver = mock(PartitionResolver.class);
    private final Enrichment enrichment = mock(Enrichment.class);
    private final UUIDGenerator uuid = mock(UUIDGenerator.class);
    private final UUID randomUUID = new UUIDGenerator().randomUUID();
    private final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
    private final FeatureToggleService featureToggleService = mock(FeatureToggleService.class);
    private final SecuritySettings settings = mock(SecuritySettings.class);
    private final ApplicationService applicationService = mock(ApplicationService.class);
    private final SubscriptionDbRepository subscriptionRepository = mock(SubscriptionDbRepository.class);

    private MockMvc mockMvc;

    @Before
    public void init() throws Exception {

        final EventTypeService eventTypeService = new EventTypeService(eventTypeRepository, topicRepository,
                partitionResolver, enrichment, uuid, featureToggleService, subscriptionRepository);

        final EventTypeOptionsValidator eventTypeOptionsValidator =
                new EventTypeOptionsValidator(TOPIC_RETENTION_MIN_MS, TOPIC_RETENTION_MAX_MS);
        final NakadiSettings nakadiSettings = new NakadiSettings(0, 0, 0, TOPIC_RETENTION_TIME_MS, 0, 60,
                NAKADI_POLL_TIMEOUT, NAKADI_SEND_TIMEOUT, 300, 5000);
        final EventTypeController controller = new EventTypeController(eventTypeService,
                featureToggleService, eventTypeOptionsValidator, applicationService, nakadiSettings);

        Mockito.doReturn(randomUUID).when(uuid).randomUUID();

        final MappingJackson2HttpMessageConverter jackson2HttpMessageConverter =
            new MappingJackson2HttpMessageConverter(objectMapper);

        doReturn(true).when(applicationService).exists(any());
        doReturn(SecuritySettings.AuthMode.OFF).when(settings).getAuthMode();
        doReturn("nakadi").when(settings).getAdminClientId();
        doReturn(false).when(featureToggleService).isFeatureEnabled(any());
        doReturn(true).when(featureToggleService).isFeatureEnabled(CHECK_PARTITIONS_KEYS);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), jackson2HttpMessageConverter)
                .setCustomArgumentResolvers(new ClientResolver(settings, featureToggleService))
                .build();

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
    public void eventTypeWithoutSchemaReturns422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        invalidEventType.setSchema(null);

        final Problem expectedProblem = invalidProblem("schema", "may not be null");

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
    public void whenPOSTWithInvalidPartitionStrategyThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito
                .doThrow(InvalidEventTypeException.class)
                .when(partitionResolver)
                .validate(any());

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTWithInvalidPartitionStrategyThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(any());

        Mockito
                .doThrow(InvalidEventTypeException.class)
                .when(partitionResolver)
                .validate(any());

        putEventType(eventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTNotOwner403() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(any());

        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS);

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "You don't have access to this event type");

        putEventType(eventType, eventType.getName(), "alice")
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTAdmin200() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(any());

        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS);

        putEventType(eventType, eventType.getName(), "nakadi")
                .andExpect(status().isOk());
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

        Mockito.doThrow(new DuplicatedEventTypeNameException("dummy message")).when(topicRepository).createTopic(any());

        postEventType(et).andExpect(status().isConflict()).andExpect(content().contentType("application/problem+json"))
                         .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeThenOk() throws Exception {

        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        Mockito.doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        Mockito.doNothing().when(eventTypeRepository).removeEventType(eventType.getName());

        Mockito.doNothing().when(topicRepository).deleteTopic(eventType.getTopic());

        deleteEventType(eventType.getName()).andExpect(status().isOk()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).removeEventType(eventType.getName());
        verify(topicRepository, times(1)).deleteTopic(eventType.getTopic());
    }

    @Test
    public void whenDeleteEventTypeThen403() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        Mockito.doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());

        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS);

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "You don't have access to this event type");

        deleteEventType(eventType.getName(), "alice")
                .andExpect(status().isForbidden())
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeAdminThen200() throws Exception {

        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        Mockito.doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());

        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();
        doReturn(true).when(featureToggleService).isFeatureEnabled(CHECK_APPLICATION_LEVEL_PERMISSIONS);

        deleteEventType(eventType.getName(), "nakadi").andExpect(status().isOk()).andExpect(content().string(""));
    }

    @Test
    public void whenDeleteNoneExistingEventTypeThen404() throws Exception {

        final String eventTypeName = randomValidEventTypeName();
        Mockito.doReturn(Optional.empty()).when(eventTypeRepository).findByNameO(eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isNotFound())
                                      .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenDeleteEventTypeAndTopicDeletionExceptionThen503() throws Exception {

        final Problem expectedProblem = Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE, "dummy message");
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        Mockito.doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        Mockito.doThrow(new TopicDeletionException("dummy message", null)).when(topicRepository).deleteTopic(
            eventType.getTopic());

        deleteEventType(eventType.getName()).andExpect(status().isServiceUnavailable())
                                      .andExpect(content().contentType("application/problem+json")).andExpect(content()
                                              .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeThatHasSubscriptionsThenConflict() throws Exception {
        final EventType eventType = buildDefaultEventType();
        when(eventTypeRepository.findByNameO(eventType.getName())).thenReturn(Optional.of(eventType));
        when(subscriptionRepository
                .listSubscriptions(eq(ImmutableSet.of(eventType.getName())), eq(Optional.empty()), anyInt(), anyInt()))
                .thenReturn(ImmutableList.of(mock(Subscription.class)));

        final Problem expectedProblem = Problem.valueOf(Response.Status.CONFLICT,
                "Not possible to remove event-type as it has subscriptions");

        deleteEventType(eventType.getName())
                .andExpect(status().isConflict())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenCreateEventTypeWithWrongPartitionKeyFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenCreateEventTypeWithUnknownApplicationThen422() throws Exception {

        doReturn(false).when(applicationService).exists(any());

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTEventTypeWithWrongPartitionKeyFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTEventTypeWithWrongPartitionKeyToBuisnesCategoryFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla"))
                .category(BUSINESS)
                .build();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenDeleteEventTypeAndNakadiExceptionThen500() throws Exception {

        final String eventTypeName = randomValidEventTypeName();
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR, "dummy message");

        Mockito.doThrow(new InternalNakadiException("dummy message")).when(eventTypeRepository).removeEventType(
            eventTypeName);
        Mockito.doReturn(Optional.of(buildDefaultEventType())).when(eventTypeRepository).findByNameO(eventTypeName);

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
    public void whenDefaultStatisticsExistsItsPassed() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        final EventTypeStatistics statistics = new EventTypeStatistics();
        statistics.setMessageSize(100);
        statistics.setMessagesPerMinute(1000);
        statistics.setReadParallelism(1);
        statistics.setWriteParallelism(2);
        defaultEventType.setDefaultStatistic(statistics);
        postEventType(defaultEventType).andExpect(status().is2xxSuccessful());
        verify(topicRepository, times(1)).createTopic(any(EventType.class));
    }

    @Test
    public void whenCreateSuccessfullyThen201() throws Exception {
        final EventType et = buildDefaultEventType();

        Mockito.doNothing().when(eventTypeRepository).saveEventType(any(EventType.class));
        Mockito.doNothing().when(topicRepository).createTopic(any());

        postEventType(et).andExpect(status().isCreated()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(any(EventType.class));
    }

    @Test
    public void whenTopicCreationFailsRemoveEventTypeFromRepositoryAnd500() throws Exception {

        final EventType et = buildDefaultEventType();
        Mockito.doNothing().when(eventTypeRepository).saveEventType(any(EventType.class));

        Mockito.doThrow(TopicCreationException.class).when(topicRepository).createTopic(any(EventType.class));

        Mockito.doNothing().when(eventTypeRepository).removeEventType(et.getName());

        final Problem expectedProblem = Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE);

        postEventType(et).andExpect(status().isServiceUnavailable())
                         .andExpect(content().contentType("application/problem+json")).andExpect(content().string(
                                 matchesProblem(expectedProblem)));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(any(EventType.class));
        verify(eventTypeRepository, times(1)).removeEventType(randomUUID.toString());
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
        final String eventTypeName = randomValidEventTypeName();
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

    @Test
    public void whenPOSTWithInvalidEnrichmentStrategyThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito
                .doThrow(InvalidEventTypeException.class)
                .when(enrichment)
                .validate(any());

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTWithInvalidEnrichmentStrategyThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        Mockito.doReturn(eventType).when(eventTypeRepository).findByName(any());

        Mockito
                .doThrow(InvalidEventTypeException.class)
                .when(enrichment)
                .validate(any());

        putEventType(eventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPostOptionsRetentionTimeExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(TOPIC_RETENTION_TIME_MS);

        postEventType(defaultEventType).andExpect(status().is2xxSuccessful());

        final ArgumentCaptor<EventType> eventTypeCaptor = ArgumentCaptor.forClass(EventType.class);
        Mockito.verify(eventTypeRepository, Mockito.times(1)).saveEventType(eventTypeCaptor.capture());
        Assert.assertEquals(TOPIC_RETENTION_TIME_MS,
                eventTypeCaptor.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenPostOptionsRetentionTimeDoesNotExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();

        postEventType(defaultEventType).andExpect(status().is2xxSuccessful());

        final ArgumentCaptor<EventType> eventTypeCaptor = ArgumentCaptor.forClass(EventType.class);
        Mockito.verify(eventTypeRepository, Mockito.times(1)).saveEventType(eventTypeCaptor.capture());
        Assert.assertEquals(TOPIC_RETENTION_TIME_MS,
                eventTypeCaptor.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenGetOptionsRetentionTimeExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(TOPIC_RETENTION_TIME_MS);
        final String eventTypeName = defaultEventType.getName();

        Mockito.doReturn(defaultEventType).when(eventTypeRepository).findByName(eventTypeName);

        getEventType(eventTypeName)
                .andExpect(status().is2xxSuccessful())
                .andExpect(content().string(new StringContains("\"options\":{\"retention_time\":150}")));
    }

    @Test
    public void whenPostOptionsRetentionTimeBiggerThanMax() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(201L);

        postEventType(defaultEventType)
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(new StringContains(
                        "Field \\\"options.retention_time\\\" can not be more than 200")));
    }

    @Test
    public void whenPostOptionsRetentionTimeSmallerThanMin() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(99L);

        postEventType(defaultEventType)
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(new StringContains(
                        "Field \\\"options.retention_time\\\" can not be less than 100")));
    }

    private ResultActions deleteEventType(final String eventTypeName) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName));
    }

    private ResultActions deleteEventType(final String eventTypeName, final String clientId) throws Exception {
        return mockMvc.perform(delete("/event-types/" + eventTypeName).principal(new UserPrincipal(clientId)));
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

    private ResultActions putEventType(final EventType eventType, final String name, final String clientId)
            throws Exception {
        final String content = objectMapper.writeValueAsString(eventType);

        return putEventType(content, name, clientId);
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

    private ResultActions putEventType(final String content, final String name, final String clientId)
            throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = put("/event-types/" + name)
                .principal(new UserPrincipal(clientId))
                .contentType(APPLICATION_JSON)
                .content(content);
        return mockMvc.perform(requestBuilder);
    }

    private ResultActions getEventType(final String eventTypeName) throws Exception {
        final MockHttpServletRequestBuilder requestBuilder = get("/event-types/" + eventTypeName);
        return mockMvc.perform(requestBuilder);
    }

    private SameJSONAs<? super String> matchesProblem(final Problem expectedProblem) throws JsonProcessingException {
        return sameJSONAs(asJsonString(expectedProblem));
    }

    private String asJsonString(final Object object) throws JsonProcessingException {
        return objectMapper.writeValueAsString(object);
    }
}
