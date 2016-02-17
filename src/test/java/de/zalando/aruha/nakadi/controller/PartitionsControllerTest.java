package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.junit.Before;
import org.junit.Test;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.zalando.problem.Problem;
import org.zalando.problem.ThrowableProblem;

import java.util.List;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;

public class PartitionsControllerTest {

    private static final String TEST_EVENT_TYPE = "test";
    private static final String UNKNOWN_EVENT_TYPE = "unknown-topic";

    private static final String TEST_PARTITION = "0";
    private static final String UNKNOWN_PARTITION = "unknown-partition";

    private static final TopicPartition TEST_TOPIC_PARTITION_0 = new TopicPartition(TEST_EVENT_TYPE, "0", "12", "67");
    private static final TopicPartition TEST_TOPIC_PARTITION_1 = new TopicPartition(TEST_EVENT_TYPE, "1", "43", "98");

    private static final List<TopicPartition> TEST_TOPIC_PARTITIONS = ImmutableList.of(
            TEST_TOPIC_PARTITION_0,
            TEST_TOPIC_PARTITION_1);

    private static final String DUMMY_MESSAGE = "dummy message";

    private TopicRepository topicRepositoryMock;

    private JsonTestHelper jsonHelper;

    private MockMvc mockMvc;

    @Before
    public void before() {
        final ObjectMapper objectMapper = new NakadiConfig().jacksonObjectMapper();
        jsonHelper = new JsonTestHelper(objectMapper);

        topicRepositoryMock = mock(TopicRepository.class);

        final PartitionsController controller = new PartitionsController(topicRepositoryMock);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .build();
    }

    @Test
    public void whenListPartitionsThenOk() throws Exception {
        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenReturn(true);
        when(topicRepositoryMock.listPartitions(eq(TEST_EVENT_TYPE))).thenReturn(TEST_TOPIC_PARTITIONS);

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(TEST_TOPIC_PARTITIONS)));
    }

    @Test
    public void whenListPartitionsForWrongTopicThenNotFound() throws Exception {
        when(topicRepositoryMock.topicExists(eq(UNKNOWN_EVENT_TYPE))).thenReturn(false);
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", UNKNOWN_EVENT_TYPE)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenListPartitionsAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        final NakadiException nakadiException = new ServiceUnavailableException("", DUMMY_MESSAGE, null);
        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenThrow(nakadiException);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, DUMMY_MESSAGE);
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions", TEST_EVENT_TYPE)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionThenOk() throws Exception {
        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenReturn(true);
        when(topicRepositoryMock.partitionExists(eq(TEST_EVENT_TYPE), eq(TEST_PARTITION))).thenReturn(true);
        when(topicRepositoryMock.getPartition(eq(TEST_EVENT_TYPE), eq(TEST_PARTITION))).thenReturn(TEST_TOPIC_PARTITION_0);

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isOk())
                .andExpect(content().string(jsonHelper.matchesObject(TEST_TOPIC_PARTITION_0)));
    }

    @Test
    public void whenGetPartitionForWrongTopicThenNotFound() throws Exception {
        when(topicRepositoryMock.topicExists(eq(UNKNOWN_EVENT_TYPE))).thenReturn(false);
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", UNKNOWN_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionForWrongPartitionThenNotFound() throws Exception {
        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenReturn(true);
        when(topicRepositoryMock.partitionExists(eq(TEST_EVENT_TYPE), eq(UNKNOWN_PARTITION))).thenReturn(false);
        final ThrowableProblem expectedProblem = Problem.valueOf(NOT_FOUND, "partition not found");

        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, UNKNOWN_PARTITION)))
                .andExpect(status().isNotFound())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

    @Test
    public void whenGetPartitionAndNakadiExceptionThenServiceUnavaiable() throws Exception {
        final NakadiException nakadiException = new ServiceUnavailableException("", DUMMY_MESSAGE, null);
        when(topicRepositoryMock.topicExists(eq(TEST_EVENT_TYPE))).thenThrow(nakadiException);

        final ThrowableProblem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE, DUMMY_MESSAGE);
        mockMvc.perform(
                get(String.format("/event-types/%s/partitions/%s", TEST_EVENT_TYPE, TEST_PARTITION)))
                .andExpect(status().isServiceUnavailable())
                .andExpect(content().string(jsonHelper.matchesObject(expectedProblem)));
    }

}
