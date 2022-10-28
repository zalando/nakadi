package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.collect.ImmutableList;
import org.hamcrest.MatcherAssert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.mockito.exceptions.base.MockitoException;
import org.springframework.http.HttpStatus;
import org.springframework.http.converter.StringHttpMessageConverter;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.domain.storage.Storage;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.kafka.KafkaPartitionStatistics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.nakadi.service.EventTypeChangeListener;
import org.zalando.nakadi.service.converter.CursorConverterImpl;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.ThreadUtils;
import org.zalando.problem.Problem;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.setup.MockMvcBuilders.standaloneSetup;
import static org.zalando.nakadi.config.SecuritySettings.AuthMode.OFF;
import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;
import static org.zalando.nakadi.utils.TestUtils.JACKSON_2_HTTP_MESSAGE_CONVERTER;
import static org.zalando.nakadi.utils.TestUtils.JSON_TEST_HELPER;
import static org.zalando.nakadi.utils.TestUtils.OBJECT_MAPPER;
import static org.zalando.nakadi.utils.TestUtils.buildDefaultEventType;
import static org.zalando.nakadi.utils.TestUtils.buildTimeline;
import static org.zalando.nakadi.utils.TestUtils.mockAccessDeniedException;
import static org.zalando.nakadi.utils.TestUtils.waitFor;
import static org.zalando.problem.Status.BAD_REQUEST;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.PRECONDITION_FAILED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

public class EventStreamControllerTest {

    private static final String TEST_EVENT_TYPE_NAME = "test";
    private static final String TEST_TOPIC = "test-topic";
    private static final EventType EVENT_TYPE = buildDefaultEventType();
    private static final String CLIENT_ID = "clientId";
    private static final Client FULL_ACCESS_CLIENT = new FullAccessClient(CLIENT_ID);
    private static final String KAFKA_CLIENT_ID = CLIENT_ID + "-" + TEST_EVENT_TYPE_NAME;

    private HttpServletRequest requestMock;
    private HttpServletResponse responseMock;
    private TopicRepository topicRepositoryMock;
    private EventStreamFactory eventStreamFactoryMock;

    private EventStreamController controller;
    private MetricRegistry metricRegistry;
    private MetricRegistry streamMetrics;
    private SecuritySettings settings;
    private EventStreamChecks eventStreamChecks;
    private EventTypeCache eventTypeCache;
    private TimelineService timelineService;
    private MockMvc mockMvc;
    private Timeline timeline;
    private AuthorizationValidator authorizationValidator;
    private EventTypeChangeListener eventTypeChangeListener;
    private AdminService adminService;
    private AuthorizationService authorizationService;

    @Before
    public void setup() throws InvalidCursorException {
        EVENT_TYPE.setName(TEST_EVENT_TYPE_NAME);
        timeline = buildTimeline(TEST_EVENT_TYPE_NAME, TEST_TOPIC, new Date());

        topicRepositoryMock = mock(TopicRepository.class);
        adminService = mock(AdminService.class);
        authorizationService = mock(AuthorizationService.class);
        when(authorizationService.getSubject()).thenReturn(Optional.empty());
        when(topicRepositoryMock.topicExists(TEST_TOPIC)).thenReturn(true);
        eventStreamFactoryMock = mock(EventStreamFactory.class);
        eventTypeCache = mock(EventTypeCache.class);
        requestMock = mock(HttpServletRequest.class);
        when(requestMock.getRemoteAddr()).thenReturn(InetAddress.getLoopbackAddress().getHostAddress());
        when(requestMock.getRemotePort()).thenReturn(12345);
        responseMock = mock(HttpServletResponse.class);

        metricRegistry = new MetricRegistry();
        streamMetrics = new MetricRegistry();
        final EventConsumer.LowLevelConsumer eventConsumerMock = mock(EventConsumer.LowLevelConsumer.class);
        when(topicRepositoryMock.createEventConsumer(eq(KAFKA_CLIENT_ID), any()))
                .thenReturn(eventConsumerMock);

        eventStreamChecks = Mockito.mock(EventStreamChecks.class);
        Mockito.when(eventStreamChecks.isConsumptionBlocked(any(), any())).thenReturn(false);

        timelineService = mock(TimelineService.class);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepositoryMock);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepositoryMock);
        when(timelineService.getTopicRepository((Storage) any())).thenReturn(topicRepositoryMock);
        when(timelineService.getActiveTimelinesOrdered(any())).thenReturn(Collections.singletonList(timeline));
        when(timelineService.getAllTimelinesOrdered(any())).thenReturn(Collections.singletonList(timeline));

        authorizationValidator = mock(AuthorizationValidator.class);
        eventTypeChangeListener = mock(EventTypeChangeListener.class);
        when(eventTypeChangeListener.registerListener(any(), any())).thenReturn(mock(Closeable.class));
        controller = new EventStreamController(
                eventTypeCache, timelineService, OBJECT_MAPPER, eventStreamFactoryMock, metricRegistry,
                streamMetrics, eventStreamChecks,
                new CursorConverterImpl(eventTypeCache, timelineService), authorizationValidator,
                eventTypeChangeListener, null);

        settings = mock(SecuritySettings.class);
        when(settings.getAuthMode()).thenReturn(OFF);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(), JACKSON_2_HTTP_MESSAGE_CONVERTER)
                .setCustomArgumentResolvers(new ClientResolver(settings, authorizationService))
                .build();
    }

    @Test
    public void testCursorsForNulls() throws Exception {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        MatcherAssert.assertThat(
                responseToString(createStreamingResponseBody("[{\"partition\":null,\"offset\":\"0\"}]")),
                JSON_TEST_HELPER.matchesObject(
                        Problem.valueOf(PRECONDITION_FAILED, "partition must not be null")));
        MatcherAssert.assertThat(
                responseToString(createStreamingResponseBody("[{\"partition\":\"0\",\"offset\":null}]")),
                JSON_TEST_HELPER.matchesObject(
                        Problem.valueOf(PRECONDITION_FAILED, "offset must not be null")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenNoParamsThenDefaultsAreUsed() throws Exception {
        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);

        final EventConsumer.LowLevelConsumer eventConsumerMock = mock(EventConsumer.LowLevelConsumer.class);
        when(topicRepositoryMock.createEventConsumer(any(), any()))
                .thenReturn(eventConsumerMock);

        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any()))
                .thenReturn(eventStreamMock);

        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        mockMvc.perform(get(String.format("/event-types/%s/events", TEST_EVENT_TYPE_NAME))
                        .header("X-nakadi-cursors", "[{\"partition\":\"0\",\"offset\":\"000000000000000000\"}]"))
                .andExpect(status().isOk());

        // we have to retry here as mockMvc exits at the very beginning, before the body starts streaming
        waitFor(() -> {
            final EventStreamConfig actualConfig = configCaptor.getValue();

            assertThat(actualConfig.getBatchLimit(), equalTo(1));
            assertThat(actualConfig.getBatchTimeout(), equalTo(30));
            assertThat(actualConfig.getCursors(),
                    equalTo(ImmutableList.of(NakadiCursor.of(timeline, "0", "000000000000000000"))));
            assertThat(actualConfig.getStreamKeepAliveLimit(), equalTo(0));
            assertThat(actualConfig.getStreamLimit(), equalTo(0));
            assertThat(actualConfig.getStreamTimeout(),
                    greaterThanOrEqualTo(EventStreamConfig.MAX_STREAM_TIMEOUT - 1200));
            assertThat(actualConfig.getStreamTimeout(),
                    lessThanOrEqualTo(EventStreamConfig.MAX_STREAM_TIMEOUT));
        }, 2000, 50, MockitoException.class);
    }

    @Test
    public void whenTopicNotExistsThenTopicNotFound() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenThrow(NoSuchEventTypeException.class);

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(20, 10, 0, 0, 0, null);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_limit can't be lower than batch_limit");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 20, 10, 0, null);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_timeout can't be lower than batch_flush_timeout");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenBatchLimitLowerThan1ThenUnprocessableEntity() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 0, 0, 0, null);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY, "batch_limit can't be lower than 1");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenWrongCursorsFormatThenBadRequest() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 0, 0, 0,
                "cursors_with_wrong_format");

        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST, "incorrect syntax of X-nakadi-cursors header");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenInvalidCursorsThenPreconditionFailed() throws Exception {
        final NakadiCursor cursor = NakadiCursor.of(timeline, "0", "000000000000000000");
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(timelineService.createEventConsumer(eq(KAFKA_CLIENT_ID), any()))
                .thenThrow(new InvalidCursorException(CursorError.UNAVAILABLE, cursor));

        final StreamingResponseBody responseBody = createStreamingResponseBody(1, 0, 0, 0, 0,
                "[{\"partition\":\"0\",\"offset\":\"00000000000000000\"}]");

        final Problem expectedProblem = Problem.valueOf(PRECONDITION_FAILED,
                "offset 000000000000000000 for partition 0 event type " + TEST_EVENT_TYPE_NAME +
                        " is unavailable as retention time of data elapsed. " +
                        "PATCH partition offset with valid and available offset");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenNoCursorsThenLatestOffsetsAreUsed() throws IOException, InvalidCursorException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        final List<PartitionStatistics> tps2 = ImmutableList.of(
                new KafkaPartitionStatistics(timeline, 0, 0, 87),
                new KafkaPartitionStatistics(timeline, 1, 0, 34));
        when(timelineService.getActiveTimeline(any(EventType.class))).thenReturn(timeline);
        when(topicRepositoryMock.loadTopicStatistics(eq(Collections.singletonList(timeline))))
                .thenReturn(tps2);

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any()))
                .thenReturn(eventStreamMock);

        final StreamingResponseBody responseBody = createStreamingResponseBody(1, 0, 1, 1, 0, null);
        responseBody.writeTo(new ByteArrayOutputStream());

        final EventStreamConfig streamConfig = configCaptor.getValue();
        assertThat(
                streamConfig.getCursors(),
                equalTo(tps2.stream().map(PartitionStatistics::getLast).collect(Collectors.toList())));
    }

    @Test
    public void whenNormalCaseThenParametersArePassedToConfigAndStreamStarted() throws Exception {
        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(timelineService.createEventConsumer(
                eq(KAFKA_CLIENT_ID), eq(ImmutableList.of(NakadiCursor.of(timeline, "0", "000000000000000000")))))
                .thenReturn(eventConsumerMock);
        when(timelineService.getActiveTimeline(eq(EVENT_TYPE))).thenReturn(timeline);

        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any()))
                .thenReturn(eventStreamMock);

        final StreamingResponseBody responseBody = createStreamingResponseBody(1, 2, 3, 4, 5,
                "[{\"partition\":\"0\",\"offset\":\"000000000000000000\"}]");
        final OutputStream outputStream = mock(OutputStream.class);
        responseBody.writeTo(outputStream);

        final EventStreamConfig streamConfig = configCaptor.getValue();
        assertThat(
                streamConfig,
                equalTo(EventStreamConfig
                        .builder()
                        .withCursors(ImmutableList.of(
                                NakadiCursor.of(timeline, "0", "000000000000000000")))
                        .withBatchLimit(1)
                        .withStreamLimit(2)
                        .withBatchTimeout(3)
                        .withStreamTimeout(4)
                        .withStreamKeepAliveLimit(5)
                        .build()
                ));

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.OK.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/x-json-stream"));

        verify(timelineService, times(1)).createEventConsumer(eq(KAFKA_CLIENT_ID),
                eq(ImmutableList.of(NakadiCursor.of(timeline, "0", "000000000000000000"))));
        verify(eventStreamFactoryMock, times(1)).createEventStream(eq(outputStream),
                eq(eventConsumerMock), eq(streamConfig), any());
        verify(eventStreamMock, times(1)).streamEvents(any());
        verify(outputStream, times(2)).flush();
        verify(outputStream, times(1)).close();
    }

    @Test
    public void whenNakadiExceptionIsThrownThenServiceUnavailable() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME))
                .thenThrow(ServiceTemporarilyUnavailableException.class);

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE);
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void whenExceptionIsThrownThenInternalServerError() throws IOException {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenThrow(NullPointerException.class);

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(INTERNAL_SERVER_ERROR);
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void reportCurrentNumberOfConsumers() throws Exception {
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        final EventStream eventStream = mock(EventStream.class);

        // block to simulate the streaming until thread is interrupted
        Mockito.doAnswer(invocation -> {
            while (!Thread.interrupted()) {
                ThreadUtils.sleep(100);
            }
            return null;
        }).when(eventStream).streamEvents(any());
        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any())).thenReturn(eventStream);

        // "connect" to the server
        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final LinkedList<Thread> clients = new LinkedList<>();
        final Counter counter = metricRegistry.counter(metricNameFor(TEST_EVENT_TYPE_NAME,
                EventStreamController.CONSUMERS_COUNT_METRIC_NAME));

        // create clients...
        for (int i = 0; i < 3; i++) {
            final Thread client = new Thread(() -> {
                try {
                    responseBody.writeTo(new ByteArrayOutputStream());
                } catch (final IOException e) {
                    throw new RuntimeException(e);
                }
            });
            client.start();
            clients.add(client);

            waitFor(
                    () -> assertThat(counter.getCount(), equalTo((long) clients.size())),
                    TimeUnit.SECONDS.toMillis(5)
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

    @Test
    public void testRead() throws Exception {
        prepareScopeRead();
        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any()))
                .thenReturn(mock(EventStream.class));

        writeStream();

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.OK.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/x-json-stream"));
    }

    @Test
    public void testAccessDenied() throws Exception {
        Mockito.doThrow(AccessDeniedException.class).when(authorizationValidator)
                .authorizeStreamRead(any());

        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        Mockito.doThrow(mockAccessDeniedException()).when(authorizationValidator).authorizeStreamRead(any());

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 0, 0, 0, null);

        final Problem expectedProblem = Problem.valueOf(FORBIDDEN, "Access on READ some-type:some-name denied");
        MatcherAssert.assertThat(responseToString(responseBody), JSON_TEST_HELPER.matchesObject(expectedProblem));
    }

    @Test
    public void testAccessAllowedForAllDataAccess() throws Exception {
        doNothing().when(authorizationValidator).authorizeStreamRead(any());

        prepareScopeRead();
        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any()))
                .thenReturn(mock(EventStream.class));

        writeStream();

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.OK.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/x-json-stream"));
        verify(authorizationValidator, times(1)).authorizeStreamRead(any());
    }

    private void writeStream() throws Exception {
        final StreamingResponseBody responseBody = createStreamingResponseBody(new NakadiClient("clientId", ""));
        final OutputStream outputStream = mock(OutputStream.class);
        responseBody.writeTo(outputStream);
    }

    private ArgumentCaptor<String> getContentTypeCaptor() {
        final ArgumentCaptor<String> contentTypeCaptor = ArgumentCaptor.forClass(String.class);
        doNothing().when(responseMock).setContentType(contentTypeCaptor.capture());
        return contentTypeCaptor;
    }

    private ArgumentCaptor<Integer> getStatusCaptor() {
        final ArgumentCaptor<Integer> statusCaptor = ArgumentCaptor.forClass(Integer.class);
        doNothing().when(responseMock).setStatus(statusCaptor.capture());
        return statusCaptor;
    }

    private void prepareScopeRead() throws InvalidCursorException {
        final EventConsumer.LowLevelConsumer eventConsumerMock = mock(EventConsumer.LowLevelConsumer.class);
        when(eventTypeCache.getEventType(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(topicRepositoryMock.createEventConsumer(
                eq(KAFKA_CLIENT_ID), eq(ImmutableList.of(NakadiCursor.of(timeline, "0", "0")))))
                .thenReturn(eventConsumerMock);
    }

    protected String responseToString(final StreamingResponseBody responseBody) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        responseBody.writeTo(out);
        return out.toString();
    }

    protected StreamingResponseBody createStreamingResponseBody() throws IOException {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, 1, 0, 0, 0, 0, null, responseMock,
                FULL_ACCESS_CLIENT);
    }

    private StreamingResponseBody createStreamingResponseBody(final Client client) throws Exception {
        return controller.streamEvents(
                TEST_EVENT_TYPE_NAME, 1, 2, 3, 4, 5, "[{\"partition\":\"0\",\"offset\":\"000000000000000000\"}]",
                responseMock, client);
    }

    private StreamingResponseBody createStreamingResponseBody(final String cursorsStr) throws Exception {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, 1, 2, 3, 4, 5, cursorsStr,
                responseMock, FULL_ACCESS_CLIENT);
    }

    private StreamingResponseBody createStreamingResponseBody(final Integer batchLimit,
                                                              final Integer streamLimit,
                                                              final Integer batchTimeout,
                                                              final Integer streamTimeout,
                                                              final Integer streamKeepAliveLimit,
                                                              final String cursorsStr) {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, batchLimit, streamLimit, batchTimeout, streamTimeout,
                streamKeepAliveLimit, cursorsStr, responseMock, FULL_ACCESS_CLIENT);
    }

}
