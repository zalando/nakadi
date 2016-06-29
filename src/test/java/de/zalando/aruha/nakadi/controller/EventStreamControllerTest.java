package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.config.JsonConfig;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.CursorError;
import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.exceptions.InvalidCursorException;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchEventTypeException;
import de.zalando.aruha.nakadi.exceptions.ServiceUnavailableException;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.EventTypeRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.service.EventStreamFactory;
import de.zalando.aruha.nakadi.utils.JsonTestHelper;
import org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.problem.Problem;

import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.LinkedList;

import static de.zalando.aruha.nakadi.metrics.MetricUtils.metricNameFor;
import static javax.ws.rs.core.Response.Status.BAD_REQUEST;
import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.PRECONDITION_FAILED;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.echocat.jomon.runtime.concurrent.RetryForSpecifiedTimeStrategy.retryForSpecifiedTimeOf;
import static org.echocat.jomon.runtime.concurrent.Retryer.executeWithRetry;
import static org.echocat.jomon.runtime.util.Duration.duration;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

public class EventStreamControllerTest {

    private static final String TEST_EVENT_TYPE_NAME = "test";
    private static final String TEST_TOPIC = "test-topic";
    private static final EventType eventType = new EventType();

    private NativeWebRequest requestMock;
    private HttpServletResponse responseMock;
    private TopicRepository topicRepositoryMock;
    private EventTypeRepository eventTypeRepository;
    private EventStreamFactory eventStreamFactoryMock;

    private ObjectMapper objectMapper;
    private EventStreamController controller;
    private JsonTestHelper jsonHelper;
    private MetricRegistry metricRegistry;

    @Before
    public void setup() {
        eventType.setName(TEST_EVENT_TYPE_NAME);
        eventType.setTopic(TEST_TOPIC);

        objectMapper = new JsonConfig().jacksonObjectMapper();
        jsonHelper = new JsonTestHelper(objectMapper);

        eventTypeRepository = mock(EventTypeRepository.class);
        topicRepositoryMock = mock(TopicRepository.class);
        eventStreamFactoryMock = mock(EventStreamFactory.class);

        metricRegistry = new MetricRegistry();
        controller = new EventStreamController(eventTypeRepository, topicRepositoryMock, objectMapper,
                eventStreamFactoryMock, metricRegistry);

        requestMock = mock(NativeWebRequest.class);
        responseMock = mock(HttpServletResponse.class);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenNoParamsThenDefaultsAreUsed() throws Exception {
        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture()))
                .thenReturn(eventStreamMock);

        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);

        final MockMvc mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .build();

        mockMvc.perform(
                get(String.format("/event-types/%s/events", TEST_EVENT_TYPE_NAME))
                        .header("X-nakadi-cursors", "[{\"partition\":\"0\",\"offset\":\"0\"}]"))
                .andExpect(status().isOk());

        final EventStreamConfig expectedConfig = EventStreamConfig
                .builder()
                .withTopic(TEST_TOPIC)
                .withBatchLimit(1)
                .withBatchTimeout(30)
                .withCursors(ImmutableMap.of("0", "0"))
                .withStreamKeepAliveLimit(0)
                .withStreamLimit(0)
                .withStreamTimeout(60)
                .build();
        // we have to retry here as mockMvc exits at the very beginning, before the body starts streaming
        executeWithRetry(() -> assertThat(configCaptor.getValue(), equalTo(expectedConfig)),
                new RetryForSpecifiedTimeStrategy<Void>(2000)
                        .withExceptionsThatForceRetry(MockitoException.class)
                        .withWaitBetweenEachTry(50));
    }

    @Test
    public void whenTopicNotExistsThenTopicNotFound() throws IOException, NakadiException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenThrow(NoSuchEventTypeException.class);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0, null,
                requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 20, 10, 0, 0, 0, null,
                requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_limit can't be lower than batch_limit");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 20, 10, 0, null,
                requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_timeout can't be lower than batch_flush_timeout");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenWrongCursorsFormatThenBadRequest() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0,
                "cursors_with_wrong_format", requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST, "incorrect syntax of X-nakadi-cursors header");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenInvalidCursorsThenPreconditionFailed() throws Exception {
        final Cursor cursor = new Cursor("0", "0");
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);
        when(topicRepositoryMock.createEventConsumer(eq(TEST_TOPIC), eq(ImmutableList.of(cursor))))
                .thenThrow(new InvalidCursorException(CursorError.UNAVAILABLE, cursor));

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0,
                "[{\"partition\":\"0\",\"offset\":\"0\"}]", requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(PRECONDITION_FAILED, "offset 0 for partition 0 is unavailable");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenNoCursorsThenLatestOffsetsAreUsed() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);
        final ImmutableList<TopicPartition> tps = ImmutableList.of(
                new TopicPartition(TEST_TOPIC, "0", "12", "87"),
                new TopicPartition(TEST_TOPIC, "1", "5", "34"));
        when(topicRepositoryMock.listPartitions(eq(TEST_TOPIC))).thenReturn(tps);

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture()))
                .thenReturn(eventStreamMock);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 1, 1, 0,
                null, requestMock, responseMock);
        responseBody.writeTo(new ByteArrayOutputStream());

        final EventStreamConfig streamConfig = configCaptor.getValue();
        assertThat(
                streamConfig.getCursors(),
                equalTo(ImmutableMap.of(
                        "0", "87",
                        "1", "34"
                )));
    }

    @Test
    public void whenNormalCaseThenParametersArePassedToConfigAndStreamStarted() throws Exception {
        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);
        when(topicRepositoryMock.createEventConsumer(eq(TEST_TOPIC), eq(ImmutableList.of(new Cursor("0", "0")))))
                .thenReturn(eventConsumerMock);

        final ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        doNothing().when(responseMock).setStatus(statusCaptor.capture());
        final ArgumentCaptor<String> contentTypeCaptor = ArgumentCaptor.forClass(String.class);
        doNothing().when(responseMock).setContentType(contentTypeCaptor.capture());

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture()))
                .thenReturn(eventStreamMock);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 1, 2, 3, 4, 5,
                "[{\"partition\":\"0\",\"offset\":\"0\"}]", requestMock, responseMock);
        final OutputStream outputStream = mock(OutputStream.class);
        responseBody.writeTo(outputStream);

        final EventStreamConfig streamConfig = configCaptor.getValue();
        assertThat(
                streamConfig,
                equalTo(EventStreamConfig
                        .builder()
                        .withTopic(TEST_TOPIC)
                        .withCursors(ImmutableMap.of("0", "0"))
                        .withBatchLimit(1)
                        .withStreamLimit(2)
                        .withBatchTimeout(3)
                        .withStreamTimeout(4)
                        .withStreamKeepAliveLimit(5)
                        .build()
                ));

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.OK.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/x-json-stream"));
        
        verify(topicRepositoryMock, times(1)).createEventConsumer(eq(TEST_TOPIC), eq(ImmutableList.of(new Cursor("0", "0"))));
        verify(eventStreamFactoryMock, times(1)).createEventStream(eq(eventConsumerMock), eq(outputStream),
                eq(streamConfig));
        verify(eventStreamMock, times(1)).streamEvents();
        verify(outputStream, times(2)).flush();
        verify(outputStream, times(1)).close();
    }

    @Test
    public void whenNakadiExceptionIsThrownThenServiceUnavailable() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenThrow(ServiceUnavailableException.class);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0, null,
                requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE);
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenExceptionIsThrownThenInternalServerError() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenThrow(NullPointerException.class);

        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0, null,
                requestMock, responseMock);

        final Problem expectedProblem = Problem.valueOf(INTERNAL_SERVER_ERROR);
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void reportCurrentNumberOfConsumers() throws Exception {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(eventType);
        final EventStream eventStream = mock(EventStream.class);

        // block to simulate the streaming until thread is interrupted
        Mockito.doAnswer(invocation -> {
            while (!Thread.interrupted()) { Thread.sleep(100); }
            return null;
        }).when(eventStream).streamEvents();
        when(eventStreamFactoryMock.createEventStream(any(), any(), any())).thenReturn(eventStream);

        // "connect" to the server
        final StreamingResponseBody responseBody = controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0, null,
                requestMock, responseMock);


        final LinkedList<Thread> clients = new LinkedList<>();
        final Counter counter = metricRegistry.counter(metricNameFor(TEST_EVENT_TYPE_NAME, EventStreamController.CONSUMERS_COUNT_METRIC_NAME));

        // create clients...
        for (int i=0; i<3; i++) {
            final Thread client = new Thread(() -> {
                try {
                    responseBody.writeTo(new ByteArrayOutputStream());
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
            client.start();
            clients.add(client);

            Thread.sleep(500);

            executeWithRetry(
                    () -> {assertThat(counter.getCount(), equalTo((long) clients.size()));},
                    retryForSpecifiedTimeOf(duration("5s"))
            );

        }

        // ...and disconnect them one by one
        while (!clients.isEmpty()) {
            final Thread client = clients.pop();
            client.interrupt();
            client.join();

            assertThat(counter.getCount(), equalTo((long) clients.size()));
        }
    }

    private String responseToString(final StreamingResponseBody responseBody) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        responseBody.writeTo(out);
        return out.toString();
    }

}
