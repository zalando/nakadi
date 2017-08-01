package org.zalando.nakadi.controller;

import com.google.common.base.Charsets;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.io.Resources;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Optional;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.Response;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.hamcrest.Matchers.containsString;
import org.hamcrest.core.StringContains;
import org.json.JSONObject;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.http.MediaType.APPLICATION_JSON;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EnrichmentStrategyDescriptor;
import static org.zalando.nakadi.domain.EventCategory.BUSINESS;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeAuthorizationAttribute;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.DuplicatedEventTypeNameException;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.TopicCreationException;
import org.zalando.nakadi.exceptions.TopicDeletionException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.UnprocessableEntityException;
import org.zalando.nakadi.exceptions.runtime.TopicConfigException;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.TestUtils;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.invalidProblem;
import static org.zalando.nakadi.utils.TestUtils.randomValidEventTypeName;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

public class EventTypeControllerTest extends EventTypeControllerTestCase {

    public EventTypeControllerTest() throws IOException {
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
        final JSONObject jsonObject = new JSONObject(TestUtils.OBJECT_MAPPER.writeValueAsString(invalidEventType));

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

        doThrow(InvalidEventTypeException.class)
                .when(partitionResolver)
                .validate(any());

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTWithInvalidPartitionStrategyThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Lists.newArrayList("invalid_key")).build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(eventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTwithPartitionStrategyChangeFromRandomToUserDefinedThenOK() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .build();

        final EventType randomEventType = EventTypeTestBuilder.builder()
                .name(eventType.getName())
                .topic(eventType.getTopic())
                .partitionStrategy(PartitionStrategy.USER_DEFINED_STRATEGY)
                .createdAt(eventType.getCreatedAt())
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(randomEventType, eventType.getName())
                .andExpect(status().isOk());
    }

    @Test
    public void whenPUTwithPartitionStrategyChangeFromRandomToHashThenOK() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .build();

        final EventType randomEventType = EventTypeTestBuilder.builder()
                .name(eventType.getName())
                .topic(eventType.getTopic())
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(Collections.singletonList("foo"))
                .createdAt(eventType.getCreatedAt())
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(randomEventType, eventType.getName())
                .andExpect(status().isOk());
    }

    @Test
    public void whenPUTwithPartitionStrategyChangeFromRandomToHashAndIncorrectKeyThen422() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .partitionKeyFields(Collections.singletonList("blabla"))
                .build();

        final EventType randomEventType = EventTypeTestBuilder.builder()
                .name(eventType.getName())
                .topic(eventType.getTopic())
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .createdAt(eventType.getCreatedAt())
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(randomEventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    public void whenPUTwithPartitionStrategyChangeFromRandomToHashAndEmptyKeyThen422() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .build();

        final EventType randomEventType = EventTypeTestBuilder.builder()
                .name(eventType.getName())
                .topic(eventType.getTopic())
                .partitionStrategy(PartitionStrategy.HASH_STRATEGY)
                .partitionKeyFields(Collections.emptyList())
                .createdAt(eventType.getCreatedAt())
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(randomEventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity());
    }

    @Test
    public void whenPUTwithPartitionStrategyChangeFromUserDefinedToRandomThen422() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionStrategy(PartitionStrategy.USER_DEFINED_STRATEGY)
                .build();

        final EventType randomEventType = EventTypeTestBuilder.builder()
                .name(eventType.getName())
                .topic(eventType.getTopic())
                .partitionStrategy(PartitionStrategy.RANDOM_STRATEGY)
                .createdAt(eventType.getCreatedAt())
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        putEventType(randomEventType, eventType.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    private void postETAndExpect422WithProblem(final EventType eventType, final Problem expectedProblem)
            throws Exception {
        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPostWithEmptyAuthorizationListThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setAuthorization(new EventTypeAuthorization(
                ImmutableList.of(), ImmutableList.of(), ImmutableList.of()));

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.admins\\\" must contain at least one attribute")))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.readers\\\" must contain at least one attribute")))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.writers\\\" must contain at least one attribute")));
    }

    @Test
    public void whenPostWithNullAuthorizationListThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setAuthorization(new EventTypeAuthorization(null, null, null));

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.admins\\\" may not be null")))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.readers\\\" may not be null")))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.writers\\\" may not be null")));
    }

    @Test
    public void whenPostAndAuthorizationInvalidThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        eventType.setAuthorization(new EventTypeAuthorization(
                ImmutableList.of(new EventTypeAuthorizationAttribute("type1", "value1")),
                ImmutableList.of(new EventTypeAuthorizationAttribute("type2", "value2")),
                ImmutableList.of(new EventTypeAuthorizationAttribute("type3", "value3"))));

        doThrow(new UnableProcessException("dummy")).when(authorizationValidator).validateAuthorization(any());

        postETAndExpect422WithProblem(eventType, Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, "dummy"));
    }

    @Test
    public void whenPostWithNullAuthAttributesFieldsThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        eventType.setAuthorization(new EventTypeAuthorization(
                ImmutableList.of(new EventTypeAuthorizationAttribute("type1", "value1")),
                ImmutableList.of(new EventTypeAuthorizationAttribute(null, "value2")),
                ImmutableList.of(new EventTypeAuthorizationAttribute("type3", null))));

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.readers[0].data_type\\\" may not be null")))
                .andExpect(content().string(
                        containsString("Field \\\"authorization.writers[0].value\\\" may not be null")));
    }

    @Test
    public void whenPostWithValidAuthorizationThenCreated() throws Exception {
        final EventType eventType = buildDefaultEventType();

        eventType.setAuthorization(new EventTypeAuthorization(
                ImmutableList.of(new EventTypeAuthorizationAttribute("type1", "value1")),
                ImmutableList.of(new EventTypeAuthorizationAttribute("type2", "value2")),
                ImmutableList.of(new EventTypeAuthorizationAttribute("type3", "value3"))));

        doReturn(eventType).when(eventTypeRepository).saveEventType(any(EventType.class));
        when(topicRepository.createTopic(anyInt(), any())).thenReturn(randomUUID.toString());

        postEventType(eventType).andExpect(status().isCreated());
    }

    @Test
    public void whenPUTAdmin200() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doReturn(eventType).when(eventTypeRepository).findByName(any());

        doReturn(SecuritySettings.AuthMode.BASIC).when(settings).getAuthMode();

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

        postETAndExpect422WithProblem(eventType, expectedProblem);
    }

    @Test
    public void whenPOSTInvalidSchemaThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getSchema().setSchema(
                "{\"not\": {\"type\": \"object\"} }");
        eventType.setCategory(BUSINESS);

        final Problem expectedProblem = new InvalidEventTypeException("Invalid schema: Invalid schema found in [#]: " +
                "extraneous key [not] is not permitted").asProblem();

        postETAndExpect422WithProblem(eventType, expectedProblem);
    }

    @Test
    public void whenPostDuplicatedEventTypeReturn409() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.CONFLICT, "some-name");

        doThrow(new DuplicatedEventTypeNameException("some-name")).when(eventTypeRepository).saveEventType(any(
                EventTypeBase.class));

        postEventType(buildDefaultEventType()).andExpect(status().isConflict())
                .andExpect(content().contentType("application/problem+json")).andExpect(
                content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeThenOk() throws Exception {

        final EventType eventType = buildDefaultEventType();

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doNothing().when(eventTypeRepository).removeEventType(eventType.getName());

        final Multimap<TopicRepository, String> topicsToDelete = ArrayListMultimap.create();
        topicsToDelete.put(topicRepository, eventType.getTopic());
        doReturn(topicsToDelete).when(timelineService).deleteAllTimelinesForEventType(eventType.getName());

        deleteEventType(eventType.getName()).andExpect(status().isOk()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).removeEventType(eventType.getName());
        verify(timelineService, times(1)).deleteAllTimelinesForEventType(eventType.getName());
    }

    @Test
    public void whenDeleteEventTypeNotAdminAndDeletionDeactivatedThenForbidden() throws Exception {
        final EventType eventType = buildDefaultEventType();

        postEventType(eventType);
        disableETDeletionFeature();

        deleteEventType(eventType.getName(), "somebody").andExpect(status().isForbidden());
    }

    @Test
    public void whenDeleteEventTypeAdminAndDeletionDeactivatedThen200() throws Exception {

        final EventType eventType = buildDefaultEventType();

        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());

        postEventType(eventType);
        disableETDeletionFeature();

        deleteEventType(eventType.getName(), "nakadi").andExpect(status().isOk()).andExpect(content().string(""));
    }

    @Test
    public void whenDeleteNoneExistingEventTypeThen404() throws Exception {

        final String eventTypeName = randomValidEventTypeName();
        doReturn(Optional.empty()).when(eventTypeRepository).findByNameO(eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isNotFound())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenDeleteEventTypeAndTopicDeletionExceptionThen503() throws Exception {

        final EventType eventType = buildDefaultEventType();
        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE,
                "Failed to delete Kafka topic for event type " + eventType.getName());

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        doReturn(Optional.of(eventType)).when(eventTypeRepository).findByNameO(eventType.getName());
        doThrow(new TopicDeletionException("dummy message", null)).when(timelineService)
                .deleteAllTimelinesForEventType(eventType.getName());

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
                "Can't remove event type " + eventType.getName() + ", as it has subscriptions");

        deleteEventType(eventType.getName())
                .andExpect(status().isConflict())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenCreateEventTypeWithWrongPartitionKeyFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenCreateEventTypeWithUnknownApplicationThen422() throws Exception {

        doReturn(false).when(applicationService).exists(any());

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        postEventType(eventType).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTEventTypeWithWrongPartitionKeyFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla")).build();

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTEventTypeWithWrongPartitionKeyToBusinessCategoryFieldsThen422() throws Exception {

        final EventType eventType = EventTypeTestBuilder.builder()
                .partitionKeyFields(Collections.singletonList("blabla"))
                .category(BUSINESS)
                .build();

        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenDeleteEventTypeAndNakadiExceptionThen500() throws Exception {

        final String eventTypeName = randomValidEventTypeName();
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR,
                "Failed to delete event type " + eventTypeName);

        doThrow(new InternalNakadiException("dummy message"))
                .when(eventTypeRepository).removeEventType(eventTypeName);
        doReturn(Optional.of(EventTypeTestBuilder.builder().name(eventTypeName).build()))
                .when(eventTypeRepository).findByNameO(eventTypeName);

        deleteEventType(eventTypeName).andExpect(status().isInternalServerError())
                .andExpect(content().contentType("application/problem+json")).andExpect(content()
                .string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPersistencyErrorThen500() throws Exception {
        final Problem expectedProblem = Problem.valueOf(Response.Status.INTERNAL_SERVER_ERROR);

        doThrow(InternalNakadiException.class).when(eventTypeRepository).saveEventType(any(EventType.class));

        postEventType(buildDefaultEventType()).andExpect(status().isInternalServerError())
                .andExpect(content().contentType("application/problem+json")).andExpect(
                content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenCreateSuccessfullyThen201() throws Exception {
        final EventType et = buildDefaultEventType();

        doReturn(et).when(eventTypeRepository).saveEventType(any(EventType.class));
        when(topicRepository.createTopic(anyInt(), any())).thenReturn(randomUUID.toString());

        postEventType(et).andExpect(status().isCreated()).andExpect(content().string(""));

        verify(eventTypeRepository, times(1)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(anyInt(), any());
    }

    @Test
    public void whenTopicCreationFailsRemoveEventTypeFromRepositoryAnd500() throws Exception {

        final EventType et = buildDefaultEventType();
        doReturn(et).when(eventTypeRepository).saveEventType(any(EventType.class));

        doThrow(TopicCreationException.class).when(topicRepository).createTopic(anyInt(), any());

        doNothing().when(eventTypeRepository).removeEventType(et.getName());

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE);

        postEventType(et).andExpect(status().isServiceUnavailable())
                .andExpect(content().contentType("application/problem+json")).andExpect(content().string(
                matchesProblem(expectedProblem)));

        verify(eventTypeRepository, times(0)).saveEventType(any(EventType.class));
        verify(topicRepository, times(1)).createTopic(anyInt(), any());
        verify(eventTypeRepository, times(0)).removeEventType(randomUUID.toString());
    }

    @Test
    public void whenPUTInvalidEventTypeThen422() throws Exception {
        final EventType invalidEventType = buildDefaultEventType();
        final JSONObject jsonObject = new JSONObject(TestUtils.OBJECT_MAPPER.writeValueAsString(invalidEventType));

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

        doReturn(eventType).when(eventTypeRepository).findByName(eventTypeName);

        putEventType(eventType, eventTypeName).andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json")).andExpect(
                content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTInexistingEventTypeThen404() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(NOT_FOUND);

        doThrow(NoSuchEventTypeException.class).when(eventTypeRepository).findByName(eventType.getName());

        putEventType(eventType, eventType.getName()).andExpect(status().isNotFound())
                .andExpect(content().contentType("application/problem+json"))
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenPUTRepoNakadiExceptionThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY);

        doThrow(UnprocessableEntityException.class).when(eventTypeRepository).findByName(eventType.getName());

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
                TestUtils.OBJECT_MAPPER.writeValueAsString(expectedEventType)));

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

        final Problem expectedProblem =
                new InvalidEventTypeException(
                        "schema must be a valid json: Unexpected token 'invalid' on line 1, char 1").asProblem();

        postETAndExpect422WithProblem(eventType, expectedProblem);
    }

    @Test
    public void invalidEventTypeSchemaJsonSchemaThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        final String jsonSchemaString = Resources.toString(Resources.getResource("sample-invalid-json-schema.json"),
                Charsets.UTF_8);
        eventType.getSchema().setSchema(jsonSchemaString);

        final Problem expectedProblem = new InvalidEventTypeException("schema must be a valid json-schema").asProblem();

        postETAndExpect422WithProblem(eventType, expectedProblem);
    }

    @Test
    public void whenPOSTWithInvalidEnrichmentStrategyThen422() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doThrow(InvalidEventTypeException.class)
                .when(enrichment)
                .validate(any());

        postEventType(eventType)
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPUTWithInvalidEnrichmentStrategyThen422() throws Exception {
        final EventTypeTestBuilder builder = EventTypeTestBuilder.builder();
        builder.enrichmentStrategies(Lists.newArrayList(EnrichmentStrategyDescriptor.METADATA_ENRICHMENT));

        final EventType original = builder.build();

        builder.enrichmentStrategies(new ArrayList<>());
        final EventType update = builder.build();

        doReturn(original).when(eventTypeRepository).findByName(any());

        putEventType(update, update.getName())
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().contentType("application/problem+json"));
    }

    @Test
    public void whenPostOptionsRetentionTimeExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(TOPIC_RETENTION_TIME_MS);

        postEventType(defaultEventType).andExpect(status().is2xxSuccessful());

        final ArgumentCaptor<EventTypeBase> eventTypeCaptor = ArgumentCaptor.forClass(EventTypeBase.class);
        verify(eventTypeRepository, times(1)).saveEventType(eventTypeCaptor.capture());
        assertEquals(TOPIC_RETENTION_TIME_MS,
                eventTypeCaptor.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenPostOptionsRetentionTimeDoesNotExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();

        postEventType(defaultEventType).andExpect(status().is2xxSuccessful());

        final ArgumentCaptor<EventTypeBase> eventTypeCaptor = ArgumentCaptor.forClass(EventTypeBase.class);
        verify(eventTypeRepository, times(1)).saveEventType(eventTypeCaptor.capture());
        assertEquals(TOPIC_RETENTION_TIME_MS,
                eventTypeCaptor.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenGetOptionsRetentionTimeExist() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(TOPIC_RETENTION_TIME_MS);
        final String eventTypeName = defaultEventType.getName();

        doReturn(defaultEventType).when(eventTypeRepository).findByName(eventTypeName);

        getEventType(eventTypeName)
                .andExpect(status().is2xxSuccessful())
                .andExpect(content().string(new StringContains("\"options\":{\"retention_time\":172800000}")));
    }

    @Test
    public void whenPostOptionsRetentionTimeBiggerThanMax() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(345600001L);

        postEventType(defaultEventType)
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(new StringContains(
                        "Field \\\"options.retention_time\\\" can not be more than 345600000")));
    }

    @Test
    public void whenPostOptionsRetentionTimeSmallerThanMin() throws Exception {
        final EventType defaultEventType = buildDefaultEventType();
        defaultEventType.getOptions().setRetentionTime(86399999L);

        postEventType(defaultEventType)
                .andExpect(status().is4xxClientError())
                .andExpect(content().string(new StringContains(
                        "Field \\\"options.retention_time\\\" can not be less than 86400000")));
    }

    @Test
    public void whenPostNullOptions201() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.setOptions(null);

        postEventType(eventType)
                .andExpect(status().isCreated());

        final ArgumentCaptor<EventTypeBase> argument = ArgumentCaptor.forClass(EventTypeBase.class);
        verify(eventTypeRepository).saveEventType(argument.capture());
        assertEquals(TOPIC_RETENTION_TIME_MS, argument.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenPostNullRetentionTime201() throws Exception {
        final EventType eventType = buildDefaultEventType();
        eventType.getOptions().setRetentionTime(null);

        postEventType(eventType)
                .andExpect(status().isCreated());

        final ArgumentCaptor<EventTypeBase> argument = ArgumentCaptor.forClass(EventTypeBase.class);
        verify(eventTypeRepository).saveEventType(argument.capture());
        assertEquals(TOPIC_RETENTION_TIME_MS, argument.getValue().getOptions().getRetentionTime().longValue());
    }

    @Test
    public void whenUpdateEventTypeAndTimelineWaitTimeoutThen503() throws Exception {
        when(timelineSync.workWithEventType(any(), anyLong())).thenThrow(new TimeoutException());
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE,
                "Event type is currently in maintenance, please repeat request");

        putEventType(eventType, eventType.getName(), "nakadi")
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDeleteEventTypeAndTimelineWaitTimeoutThen503() throws Exception {
        when(timelineSync.workWithEventType(any(), anyLong())).thenThrow(new TimeoutException());
        final EventType eventType = buildDefaultEventType();

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE,
                "Event type " + eventType.getName() + " is currently in maintenance, please repeat request");

        deleteEventType(eventType.getName())
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(matchesProblem(expectedProblem)));
    }

    @Test
    public void whenDuplicatedEventTypeThenTopicIsRemoved() throws Exception {
        final EventType eventType = buildDefaultEventType();

        doThrow(DuplicatedEventTypeNameException.class)
                .when(eventTypeRepository).saveEventType(any(EventType.class));
        when(topicRepository.createTopic(0, 172800000L)).thenReturn("test-topic");

        postEventType(eventType)
                .andExpect(status().isConflict())
                .andExpect(content().string(matchesProblem(Problem.valueOf(Response.Status.CONFLICT))));

        verify(topicRepository).deleteTopic("test-topic");
    }

    @Test
    public void whenUpdateRetentionTimeAndKafkaFails() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(172800000L);
        eventType.setOptions(eventTypeOptions);
        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        doThrow(TopicConfigException.class).when(topicRepository).setRetentionTime(anyString(), anyLong());
        when(timelineService.getActiveTimelinesOrdered(any()))
                .thenReturn(Collections.singletonList(
                        Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date())));

        final EventType eventType2 = EventTypeTestBuilder.builder().name(eventType.getName()).build();
        final EventTypeOptions eventTypeOptions2 = new EventTypeOptions();
        eventTypeOptions2.setRetentionTime(172800001L);
        eventType2.setOptions(eventTypeOptions2);

        putEventType(eventType2, eventType2.getName(), "nakadi")
                .andExpect(status().isInternalServerError());
        verify(topicRepository, times(2)).setRetentionTime(anyString(), anyLong());
        verify(eventTypeRepository, times(0)).update(any());
    }

    @Test
    public void whenUpdateRetentionTimeAndDbFails() throws Exception {
        final EventType eventType = EventTypeTestBuilder.builder().build();
        final EventTypeOptions eventTypeOptions = new EventTypeOptions();
        eventTypeOptions.setRetentionTime(172800000L);
        eventType.setOptions(eventTypeOptions);
        doReturn(eventType).when(eventTypeRepository).findByName(eventType.getName());
        doThrow(InternalNakadiException.class).when(eventTypeRepository).update(any());
        when(timelineService.getActiveTimelinesOrdered(any()))
                .thenReturn(Collections.singletonList(
                        Timeline.createTimeline(eventType.getName(), 0, null, "topic", new Date())));

        final EventType eventType2 = EventTypeTestBuilder.builder().name(eventType.getName()).build();
        final EventTypeOptions eventTypeOptions2 = new EventTypeOptions();
        eventTypeOptions2.setRetentionTime(172800001L);
        eventType2.setOptions(eventTypeOptions2);

        putEventType(eventType2, eventType2.getName(), "nakadi")
                .andExpect(status().isInternalServerError());
        verify(topicRepository, times(2)).setRetentionTime(anyString(), anyLong());
        verify(eventTypeRepository).update(any());
    }

}
