package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.controller.advice.NakadiProblemExceptionHandler;
import org.zalando.nakadi.controller.advice.PartitionsExceptionHandler;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.ResourceImpl;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.repository.kafka.KafkaPartitionStatistics;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.RepartitioningService;
import org.zalando.nakadi.service.converter.CursorConverterImpl;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class PartitionsControllerTest {

    private static final String TEST_EVENT_TYPE = "test";
    private static final String UNKNOWN_EVENT_TYPE = "unknown-topic";

    private static final String TEST_PARTITION = "0";
    private static final String UNKNOWN_PARTITION = "unknown-partition";

    private static final EventTypePartitionView TEST_TOPIC_PARTITION_0 =
            new EventTypePartitionView("0", "001-0000-000000000000000012",
                    "001-0000-000000000000000067");
    private static final EventTypePartitionView TEST_TOPIC_PARTITION_1 =
            new EventTypePartitionView("1", "001-0000-000000000000000043",
                    "001-0000-000000000000000098");

    private static final EventTypePartitionView TEST_TOPIC_PARTITION_0_UE =
            new EventTypePartitionView("0", "001-0000-000000000000000012",
                    "001-0000-000000000000000067", 2L);
    private static final EventTypePartitionView TEST_TOPIC_PARTITION_1_UE=
            new EventTypePartitionView("1", "001-0000-000000000000000043",
                    "001-0000-000000000000000098", 2L);

    private static final String CURSORS_TEMPLATE = "[\n" +
            "    {\n" +
            "        \"partition\": \"0\",\n" +
            "        \"offset\": \"1\"\n" +
            "    },\n" +
            "    {\n" +
            "        \"partition\": \"%s\",\n" +
            "        \"offset\": \"%s\" \n" +
            "    }\n" +
            "]";

    private static final List<EventTypePartitionView> TEST_TOPIC_PARTITIONS = ImmutableList.of(
            TEST_TOPIC_PARTITION_0,
            TEST_TOPIC_PARTITION_1);

    private static final List<EventTypePartitionView> TEST_TOPIC_PARTITIONS_UE = ImmutableList.of(
            TEST_TOPIC_PARTITION_0_UE,
            TEST_TOPIC_PARTITION_1_UE);

    private static final EventType EVENT_TYPE = TestUtils.buildDefaultEventType();
    private static final Timeline TIMELINE = TestUtils.buildTimeline(EVENT_TYPE.getName());

    private static final List<PartitionStatistics> TEST_POSITION_STATS = ImmutableList.of(
            new KafkaPartitionStatistics(TIMELINE, 0, 12, 67),
            new KafkaPartitionStatistics(TIMELINE, 1, 43, 98));
    private final AuthorizationValidator authorizationValidator = mock(AuthorizationValidator.class);
    private EventTypeCache eventTypeCacheMock;
    private TopicRepository topicRepositoryMock;
    private EventTypeCache eventTypeCache;
    private TimelineService timelineService;
    private CursorOperationsService cursorOperationsService;
    private MockMvc mockMvc;
    private SecuritySettings settings;
    private AuthorizationService authorizationService;
    private static final ObjectMapper OBJECT_MAPPER = TestUtils.OBJECT_MAPPER;

    @Before
    public void before() throws InternalNakadiException, NoSuchEventTypeException {
        eventTypeCacheMock = mock(EventTypeCache.class);
        topicRepositoryMock = mock(TopicRepository.class);
        eventTypeCache = mock(EventTypeCache.class);
        timelineService = mock(TimelineService.class);
        cursorOperationsService = spy(new CursorOperationsService(timelineService));
        authorizationService = mock(AuthorizationService.class);
        Mockito.when(authorizationService.getSubject()).thenReturn(Optional.empty());
        Mockito.when(timelineService.getActiveTimelinesOrdered(eq(UNKNOWN_EVENT_TYPE)))
                .thenThrow(new NoSuchEventTypeException("topic not found"));
        Mockito.when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenReturn(Collections.singletonList(TIMELINE));
        Mockito.when(timelineService.getAllTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenReturn(Collections.singletonList(TIMELINE));
        Mockito.when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepositoryMock);
        final CursorConverter cursorConverter = new CursorConverterImpl(eventTypeCache, timelineService);
        final PartitionsController controller = new PartitionsController(timelineService, cursorConverter,
                cursorOperationsService, eventTypeCacheMock, mock(EventTypeRepository.class), authorizationValidator,
                        mock(AdminService.class), mock(RepartitioningService.class), OBJECT_MAPPER);

        settings = mock(SecuritySettings.class);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, authorizationService))
                .setControllerAdvice(new NakadiProblemExceptionHandler(), new PartitionsExceptionHandler())
                .build();
    }

    @Test
    public void whenListPartitionsThenOk() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadTopicStatistics(
                eq(Collections.singletonList(TIMELINE))))
                .thenReturn(TEST_POSITION_STATS);

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(TEST_TOPIC_PARTITIONS)));
    }

    @Test
    public void whenListPartitionsForWrongTopicThenNotFound() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", UNKNOWN_EVENT_TYPE)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        Mockito.when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenThrow(ServiceTemporarilyUnavailableException.class);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "");
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionThenOk() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadPartitionStatistics(eq(TIMELINE), eq(TEST_PARTITION)))
                .thenReturn(Optional.of(TEST_POSITION_STATS.get(0)));

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(TEST_TOPIC_PARTITION_0)));
    }

    @Test
    public void whenUnauthorizedGetPartitionThenForbiddenStatusCode() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.doThrow(TestUtils.mockAccessDeniedException()).when(authorizationValidator).authorizeStreamRead(any());

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isForbidden());
    }

    @Test
    public void whenGetPartitionUnauthorisedViewThenError() throws Exception {
        final ResourceImpl<String> resource = new ResourceImpl("some_user", "user", null, "");
        final ThrowableProblem expectedProblem = Problem.valueOf(FORBIDDEN, "Access on VIEW user:some_user denied");

        Mockito.doThrow(new AccessDeniedException(AuthorizationService.Operation.VIEW, resource)).
                when(authorizationValidator).authorizeEventTypeView(EVENT_TYPE);

        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadPartitionStatistics(eq(TIMELINE), eq(TEST_PARTITION)))
                .thenReturn(Optional.of(TEST_POSITION_STATS.get(0)));

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isForbidden())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionWithConsumedOffsetThenOk() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadPartitionStatistics(eq(TIMELINE), eq(TEST_PARTITION)))
                .thenReturn(Optional.of(TEST_POSITION_STATS.get(0)));
        final List<NakadiCursorLag> lags = mockCursorLag();
        Mockito.doReturn(lags)
                .when(cursorOperationsService)
                .cursorsLag(any(), anyList());

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s?consumed_offset=1", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isOk())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(new EventTypePartitionView(
                        "0",
                        "001-0000-0",
                        "001-0000-1",
                        42L
                ))));
    }

    private List<NakadiCursorLag> mockCursorLag() {
        final Timeline timeline = mock(Timeline.class);
        Mockito.when(timeline.getStorage()).thenReturn(new Storage("ccc", Storage.Type.KAFKA));
        final NakadiCursorLag lag = mock(NakadiCursorLag.class);
        final NakadiCursor firstCursor = NakadiCursor.of(timeline, "0", "0");
        final NakadiCursor lastCursor = NakadiCursor.of(timeline, "0", "1");
        Mockito.when(lag.getLag()).thenReturn(42L);
        Mockito.when(lag.getPartition()).thenReturn("0");
        Mockito.when(lag.getFirstCursor()).thenReturn(firstCursor);
        Mockito.when(lag.getLastCursor()).thenReturn(lastCursor);
        return Lists.newArrayList(lag);
    }

    @Test
    public void whenGetPartitionForWrongTopicThenNotFound() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(UNKNOWN_EVENT_TYPE))
                .thenThrow(new NoSuchEventTypeException("topic not found"));
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", UNKNOWN_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionForWrongPartitionThenNotFound() throws Exception {
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadPartitionStatistics(
                eq(TIMELINE), eq(UNKNOWN_PARTITION)))
                .thenReturn(Optional.empty());
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "partition not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, UNKNOWN_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        Mockito.when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenThrow(ServiceTemporarilyUnavailableException.class);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, "");
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionsFromIncorrectConsumedOffsetThenUnprocessable() throws Exception {
        final String argUnderTest = "xer";
        final ThrowableProblem expectedProblem =
                Problem.valueOf(
                        UNPROCESSABLE_ENTITY,
                        String.format("invalid offset %s for partition 0", argUnderTest));
        mockMvc.perform(
                get("/event-types/{0}/partitions/0?consumed_offset={1}", TEST_EVENT_TYPE, argUnderTest))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsFromIncompleteCursorOffsetThenUnprocessable() throws Exception {
        final ThrowableProblem expectedProblem =
                Problem.valueOf(UNPROCESSABLE_ENTITY, "offset must not be null");
        final String cursorString = String.format(CURSORS_TEMPLATE, "1", "");
        mockMvc.perform(
                    get("/event-types/{0}/partitions?cursors={1}", TEST_EVENT_TYPE, cursorString))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsFromIncompleteCursorPartitionThenUnprocessable() throws Exception {
        final ThrowableProblem expectedProblem =
                Problem.valueOf(UNPROCESSABLE_ENTITY, "partition must not be null");
        final String cursorString = String.format(CURSORS_TEMPLATE, "", "1");
        mockMvc.perform(
                    get("/event-types/{0}/partitions?cursors={1}", TEST_EVENT_TYPE, cursorString))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsFromRepeatedPartitionsThenUnprocessable() throws Exception {
        final ThrowableProblem expectedProblem =
                Problem.valueOf(UNPROCESSABLE_ENTITY, "duplicate partition ids provided in cursors");
        final String cursorString = String.format(CURSORS_TEMPLATE, "0", "1");
        mockMvc.perform(
                    get("/event-types/{0}/partitions?cursors={1}", TEST_EVENT_TYPE, cursorString))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsFromMalformedCursorsThenUnprocessable() throws Exception {
        final ThrowableProblem expectedProblem =
                Problem.valueOf(UNPROCESSABLE_ENTITY, "malformed cursors");
        final String cursorString = "xyz";
        mockMvc.perform(
                    get("/event-types/{0}/partitions?cursors={1}", TEST_EVENT_TYPE, cursorString))
                .andExpect(status().isUnprocessableEntity())
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsWithCursorsThenOk() throws Exception {
        Mockito.doNothing().when(authorizationValidator).authorizeStreamRead(any());
        Mockito.when(eventTypeCacheMock.getEventType(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        Mockito.when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getName()))).thenReturn(true);
        Mockito.when(topicRepositoryMock.loadTopicStatistics(
                eq(Collections.singletonList(TIMELINE))))
                .thenReturn(TEST_POSITION_STATS);

        final List<NakadiCursorLag> cursorLagList = getNakadiCursorLags();
        Mockito.doReturn(cursorLagList)
                .when(cursorOperationsService)
                .cursorsLag(any(), anyList());
        final String cursorString = String.format(CURSORS_TEMPLATE, "1", "0");
        mockMvc.perform(
                    get("/event-types/{0}/partitions?cursors={1}", TEST_EVENT_TYPE, cursorString))
                .andExpect(content().string(TestUtils.JSON_TEST_HELPER.matchesObject(TEST_TOPIC_PARTITIONS_UE)));
    }

    private List<NakadiCursorLag> getNakadiCursorLags() {
        final UnaryOperator<String> getOffset = (fullOffset) -> fullOffset.split("-", -1)[2];

        final Function<EventTypePartitionView, NakadiCursor> getFirstCursor = (view) ->
                NakadiCursor.of(TIMELINE, view.getPartitionId(), getOffset.apply(view.getOldestAvailableOffset()));

        final Function<EventTypePartitionView, NakadiCursor> getLastCursor = (view) ->
                NakadiCursor.of(TIMELINE, view.getPartitionId(), getOffset.apply(view.getNewestAvailableOffset()));

        final Function<EventTypePartitionView, NakadiCursorLag> getLag = (view) ->
                new NakadiCursorLag(
                    getFirstCursor.apply(view),
                    getLastCursor.apply(view),
                    view.getUnconsumedEvents()
                );

        return Arrays.asList(getLag.apply(TEST_TOPIC_PARTITION_0_UE), getLag.apply(TEST_TOPIC_PARTITION_1_UE));
    }

}
