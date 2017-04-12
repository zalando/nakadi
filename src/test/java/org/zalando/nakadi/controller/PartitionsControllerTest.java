package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.kafka.KafkaPartitionStatistics;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.converter.CursorConverterImpl;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;

public class PartitionsControllerTest {

    private static final String TEST_EVENT_TYPE = "test";
    private static final String UNKNOWN_EVENT_TYPE = "unknown-topic";

    private static final String TEST_PARTITION = "0";
    private static final String UNKNOWN_PARTITION = "unknown-partition";

    private static final EventTypePartitionView TEST_TOPIC_PARTITION_0 =
            new EventTypePartitionView(TEST_EVENT_TYPE, "0", "000000000000000012", "000000000000000067");
    private static final EventTypePartitionView TEST_TOPIC_PARTITION_1 =
            new EventTypePartitionView(TEST_EVENT_TYPE, "1", "000000000000000043", "000000000000000098");

    private static final List<EventTypePartitionView> TEST_TOPIC_PARTITIONS = ImmutableList.of(
            TEST_TOPIC_PARTITION_0,
            TEST_TOPIC_PARTITION_1);

    private static final EventType EVENT_TYPE = TestUtils.buildDefaultEventType();
    private static final Timeline TIMELINE = createFakeTimeline(EVENT_TYPE.getTopic());

    private static final List<PartitionStatistics> TEST_POSITION_STATS = ImmutableList.of(
            new KafkaPartitionStatistics(TIMELINE, 0, 12, 67),
            new KafkaPartitionStatistics(TIMELINE, 1, 43, 98));


    private EventTypeRepository eventTypeRepositoryMock;

    private TopicRepository topicRepositoryMock;

    private EventTypeCache eventTypeCache;

    private TimelineService timelineService;

    private JsonTestHelper jsonHelper;

    private MockMvc mockMvc;

    @Before
    public void before() throws InternalNakadiException, NoSuchEventTypeException {
        final ObjectMapper objectMapper = new JsonConfig().jacksonObjectMapper();
        jsonHelper = new JsonTestHelper(objectMapper);

        eventTypeRepositoryMock = mock(EventTypeRepository.class);
        topicRepositoryMock = mock(TopicRepository.class);
        eventTypeCache = mock(EventTypeCache.class);
        timelineService = Mockito.mock(TimelineService.class);
        when(timelineService.getActiveTimelinesOrdered(eq(UNKNOWN_EVENT_TYPE)))
                .thenThrow(NoSuchEventTypeException.class);
        when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenReturn(Collections.singletonList(TIMELINE));
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepositoryMock);
        final CursorConverter cursorConverter = new CursorConverterImpl(eventTypeCache, timelineService);
        final PartitionsController controller = new PartitionsController(timelineService, cursorConverter);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    public void whenListPartitionsThenOk() throws Exception {
        when(eventTypeRepositoryMock.findByName(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getTopic()))).thenReturn(true);
        when(topicRepositoryMock.loadTopicStatistics(
                eq(Collections.singletonList(TIMELINE))))
                .thenReturn(TEST_POSITION_STATS);

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(TEST_TOPIC_PARTITIONS)));
    }

    @Test
    public void whenListPartitionsForWrongTopicThenNotFound() throws Exception {
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", UNKNOWN_EVENT_TYPE)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenThrow(ServiceUnavailableException.class);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, null);
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionThenOk() throws Exception {
        when(eventTypeRepositoryMock.findByName(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getTopic()))).thenReturn(true);
        when(topicRepositoryMock.loadPartitionStatistics(eq(TIMELINE), eq(TEST_PARTITION)))
                .thenReturn(Optional.of(TEST_POSITION_STATS.get(0)));

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(TEST_TOPIC_PARTITION_0)));
    }

    @Test
    public void whenGetPartitionForWrongTopicThenNotFound() throws Exception {
        when(eventTypeRepositoryMock.findByName(UNKNOWN_EVENT_TYPE)).thenThrow(NoSuchEventTypeException.class);
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", UNKNOWN_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionForWrongPartitionThenNotFound() throws Exception {
        when(eventTypeRepositoryMock.findByName(TEST_EVENT_TYPE)).thenReturn(EVENT_TYPE);
        when(topicRepositoryMock.topicExists(eq(EVENT_TYPE.getTopic()))).thenReturn(true);
        when(topicRepositoryMock.loadPartitionStatistics(
                eq(TIMELINE), eq(UNKNOWN_PARTITION)))
                .thenReturn(Optional.empty());
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "partition not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, UNKNOWN_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        when(timelineService.getActiveTimelinesOrdered(eq(TEST_EVENT_TYPE)))
                .thenThrow(ServiceUnavailableException.class);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, null);
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

}
