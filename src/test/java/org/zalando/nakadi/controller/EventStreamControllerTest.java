package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
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
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.config.JsonConfig;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.repository.db.EventTypeCache;
import org.zalando.nakadi.repository.kafka.KafkaPartitionStatistics;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.security.ClientResolver;
import org.zalando.nakadi.security.FullAccessClient;
import org.zalando.nakadi.security.NakadiClient;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.ConsumerLimitingService;
import org.zalando.nakadi.service.EventStream;
import org.zalando.nakadi.service.EventStreamConfig;
import org.zalando.nakadi.service.EventStreamFactory;
import org.zalando.nakadi.service.converter.CursorConverterImpl;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.nakadi.utils.JsonTestHelper;
import org.zalando.nakadi.utils.TestUtils;
import org.zalando.problem.Problem;
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
import static org.zalando.nakadi.metrics.MetricUtils.metricNameFor;
import static org.zalando.nakadi.utils.TestUtils.createFakeTimeline;
import static org.zalando.problem.MoreStatus.UNPROCESSABLE_ENTITY;

public class EventStreamControllerTest {

    private static final String TEST_EVENT_TYPE_NAME = "test";
    private static final String TEST_TOPIC = "test-topic";
    private static final EventType EVENT_TYPE = TestUtils.buildDefaultEventType();
    private static final Set<String> SCOPE_READ = Collections.singleton("oauth2.scope.read");
    private static final String CLIENT_ID = "clientId";
    private static final Client FULL_ACCESS_CLIENT = new FullAccessClient(CLIENT_ID);
    private static final String KAFKA_CLIENT_ID = CLIENT_ID + "-" + TEST_EVENT_TYPE_NAME;

    private HttpServletRequest requestMock;
    private HttpServletResponse responseMock;
    private TopicRepository topicRepositoryMock;
    private EventTypeRepository eventTypeRepository;
    private EventStreamFactory eventStreamFactoryMock;

    private ObjectMapper objectMapper;
    private EventStreamController controller;
    private JsonTestHelper jsonHelper;
    private MetricRegistry metricRegistry;
    private MetricRegistry streamMetrics;
    private FeatureToggleService featureToggleService;
    private SecuritySettings settings;
    private BlacklistService blacklistService;
    private EventTypeCache eventTypeCache;
    private TimelineService timelineService;
    private MockMvc mockMvc;
    private Timeline fakeTimeline;

    @Before
    public void setup() throws NakadiException, UnknownHostException, InvalidCursorException {
        EVENT_TYPE.setName(TEST_EVENT_TYPE_NAME);
        EVENT_TYPE.setTopic(TEST_TOPIC);

        objectMapper = new JsonConfig().jacksonObjectMapper();
        jsonHelper = new JsonTestHelper(objectMapper);

        fakeTimeline = createFakeTimeline(TEST_TOPIC);

        eventTypeRepository = mock(EventTypeRepository.class);
        topicRepositoryMock = mock(TopicRepository.class);
        when(topicRepositoryMock.topicExists(TEST_TOPIC)).thenReturn(true);
        eventStreamFactoryMock = mock(EventStreamFactory.class);
        eventTypeCache = mock(EventTypeCache.class);
        requestMock = mock(HttpServletRequest.class);
        when(requestMock.getRemoteAddr()).thenReturn(InetAddress.getLoopbackAddress().getHostAddress());
        when(requestMock.getRemotePort()).thenReturn(12345);
        responseMock = mock(HttpServletResponse.class);

        metricRegistry = new MetricRegistry();
        streamMetrics = new MetricRegistry();
        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(topicRepositoryMock.createEventConsumer(
                eq(KAFKA_CLIENT_ID), any()))
                .thenReturn(eventConsumerMock);

        final ClosedConnectionsCrutch crutch = mock(ClosedConnectionsCrutch.class);
        when(crutch.listenForConnectionClose(requestMock)).thenReturn(new AtomicBoolean(true));

        blacklistService = Mockito.mock(BlacklistService.class);
        Mockito.when(blacklistService.isConsumptionBlocked(any(), any())).thenReturn(false);

        final ConsumerLimitingService consumerLimitingService = Mockito.mock(ConsumerLimitingService.class);
        when(consumerLimitingService.acquireConnectionSlots(any(), any(), any())).thenReturn(ImmutableList.of());

        featureToggleService = mock(FeatureToggleService.class);
        timelineService = mock(TimelineService.class);
        when(timelineService.getTopicRepository((Timeline) any())).thenReturn(topicRepositoryMock);
        when(timelineService.getTopicRepository((EventTypeBase) any())).thenReturn(topicRepositoryMock);
        when(timelineService.getFakeTimeline(any())).thenReturn(fakeTimeline);

        controller = new EventStreamController(
                eventTypeRepository, timelineService, objectMapper, eventStreamFactoryMock, metricRegistry,
                streamMetrics, crutch, blacklistService, consumerLimitingService, featureToggleService,
                new CursorConverterImpl(eventTypeCache, timelineService));

        settings = mock(SecuritySettings.class);

        mockMvc = standaloneSetup(controller)
                .setMessageConverters(new StringHttpMessageConverter(),
                        new MappingJackson2HttpMessageConverter(objectMapper))
                .setCustomArgumentResolvers(new ClientResolver(settings, featureToggleService))
                .build();
    }

    @Test
    public void testCursorsForNulls() throws Exception {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        assertThat(
                responseToString(createStreamingResponseBody("[{\"partition\":null,\"offset\":\"0\"}]")),
                jsonHelper.matchesObject(Problem.valueOf(PRECONDITION_FAILED, "partition must not be null")));
        assertThat(
                responseToString(createStreamingResponseBody("[{\"partition\":\"0\",\"offset\":null}]")),
                jsonHelper.matchesObject(Problem.valueOf(PRECONDITION_FAILED, "offset must not be null")));
    }

    @Test
    @SuppressWarnings("unchecked")
    public void whenNoParamsThenDefaultsAreUsed() throws Exception {
        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);

        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(topicRepositoryMock.createEventConsumer(
                any(), any()))
                .thenReturn(eventConsumerMock);

        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any(), any(), any()))
                .thenReturn(eventStreamMock);

        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        mockMvc.perform(
                get(String.format("/event-types/%s/events", TEST_EVENT_TYPE_NAME))
                        .header("X-nakadi-cursors", "[{\"partition\":\"0\",\"offset\":\"000000000000000000\"}]"))
                .andExpect(status().isOk());

        final EventStreamConfig expectedConfig = EventStreamConfig
                .builder()
                .withBatchLimit(1)
                .withBatchTimeout(30)
                .withCursors(ImmutableList.of(
                        new NakadiCursor(fakeTimeline, "0", "000000000000000000")))
                .withStreamKeepAliveLimit(0)
                .withStreamLimit(0)
                .withStreamTimeout(0)
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

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(NOT_FOUND, "topic not found");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamLimitLowerThanBatchLimitThenUnprocessableEntity() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(20, 10, 0, 0, 0, null);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_limit can't be lower than batch_limit");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenStreamTimeoutLowerThanBatchTimeoutThenUnprocessableEntity() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 20, 10, 0, null);

        final Problem expectedProblem = Problem.valueOf(UNPROCESSABLE_ENTITY,
                "stream_timeout can't be lower than batch_flush_timeout");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenWrongCursorsFormatThenBadRequest() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 0, 0, 0,
                "cursors_with_wrong_format");

        final Problem expectedProblem = Problem.valueOf(BAD_REQUEST, "incorrect syntax of X-nakadi-cursors header");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenInvalidCursorsThenPreconditionFailed() throws Exception {
        final NakadiCursor cursor = new NakadiCursor(fakeTimeline, "0", "000000000000000000");
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(timelineService.createEventConsumer(eq(KAFKA_CLIENT_ID), any()))
                .thenThrow(new InvalidCursorException(CursorError.UNAVAILABLE, cursor));

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 0, 0, 0,
                "[{\"partition\":\"0\",\"offset\":\"00000000000000000\"}]");

        final Problem expectedProblem = Problem.valueOf(PRECONDITION_FAILED,
                "offset 000000000000000000 for partition 0 is unavailable");
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenNoCursorsThenLatestOffsetsAreUsed() throws NakadiException, IOException, InvalidCursorException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        final List<PartitionStatistics> tps2 = ImmutableList.of(
                new KafkaPartitionStatistics(fakeTimeline, 0, 0, 87),
                new KafkaPartitionStatistics(fakeTimeline, 1, 0, 34));
        when(timelineService.getTimeline(any())).thenReturn(fakeTimeline);
        when(topicRepositoryMock.loadTopicStatistics(eq(Collections.singletonList(fakeTimeline))))
                .thenReturn(tps2);

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any(), any(), any()))
                .thenReturn(eventStreamMock);

        final StreamingResponseBody responseBody = createStreamingResponseBody(0, 0, 1, 1, 0, null);
        responseBody.writeTo(new ByteArrayOutputStream());

        final EventStreamConfig streamConfig = configCaptor.getValue();
        assertThat(
                streamConfig.getCursors(),
                equalTo(tps2.stream().map(PartitionStatistics::getLast).collect(Collectors.toList())));
    }

    @Test
    public void whenNormalCaseThenParametersArePassedToConfigAndStreamStarted() throws Exception {
        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(timelineService.createEventConsumer(
                eq(KAFKA_CLIENT_ID), eq(ImmutableList.of(new NakadiCursor(fakeTimeline, "0", "000000000000000000")))))
                .thenReturn(eventConsumerMock);
        when(timelineService.getTimeline(eq(EVENT_TYPE))).thenReturn(fakeTimeline);

        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        final ArgumentCaptor<EventStreamConfig> configCaptor = ArgumentCaptor.forClass(EventStreamConfig.class);
        final EventStream eventStreamMock = mock(EventStream.class);
        when(eventStreamFactoryMock.createEventStream(any(), any(), configCaptor.capture(), any(), any(), any()))
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
                                new NakadiCursor(fakeTimeline, "0", "000000000000000000")))
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
                eq(ImmutableList.of(new NakadiCursor(fakeTimeline, "0", "000000000000000000"))));
        verify(eventStreamFactoryMock, times(1)).createEventStream(eq(outputStream),
                eq(eventConsumerMock), eq(streamConfig), any(), any(), any());
        verify(eventStreamMock, times(1)).streamEvents(any());
        verify(outputStream, times(2)).flush();
        verify(outputStream, times(1)).close();
    }

    @Test
    public void whenNakadiExceptionIsThrownThenServiceUnavailable() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenThrow(ServiceUnavailableException.class);

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(SERVICE_UNAVAILABLE);
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void whenExceptionIsThrownThenInternalServerError() throws NakadiException, IOException {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenThrow(NullPointerException.class);

        final StreamingResponseBody responseBody = createStreamingResponseBody();

        final Problem expectedProblem = Problem.valueOf(INTERNAL_SERVER_ERROR);
        assertThat(responseToString(responseBody), jsonHelper.matchesObject(expectedProblem));
    }

    @Test
    public void reportCurrentNumberOfConsumers() throws Exception {
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        final EventStream eventStream = mock(EventStream.class);

        // block to simulate the streaming until thread is interrupted
        Mockito.doAnswer(invocation -> {
            while (!Thread.interrupted()) {
                Thread.sleep(100);
            }
            return null;
        }).when(eventStream).streamEvents(any());
        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any(), any(), any()))
                .thenReturn(eventStream);

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

            Thread.sleep(500);

            executeWithRetry(
                    () -> assertThat(counter.getCount(), equalTo((long) clients.size())),
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

    @Test
    public void testReadScope() throws Exception {
        prepareScopeRead();
        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any(), any(), any()))
                .thenReturn(mock(EventStream.class));

        writeStream(SCOPE_READ);

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.OK.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/x-json-stream"));

        clearScopes();
    }

    @Test
    public void testNoReadScope() throws Exception {
        prepareScopeRead();

        final ArgumentCaptor<Integer> statusCaptor = getStatusCaptor();
        final ArgumentCaptor<String> contentTypeCaptor = getContentTypeCaptor();

        when(eventStreamFactoryMock.createEventStream(any(), any(), any(), any(), any(), any()))
                .thenReturn(mock(EventStream.class));

        writeStream(Collections.emptySet());

        assertThat(statusCaptor.getValue(), equalTo(HttpStatus.FORBIDDEN.value()));
        assertThat(contentTypeCaptor.getValue(), equalTo("application/problem+json"));

        clearScopes();
    }

    private void clearScopes() {
        EVENT_TYPE.setReadScopes(Collections.emptySet());
    }

    private void writeStream(final Set<String> scopes) throws Exception {
        final StreamingResponseBody responseBody = createStreamingResponseBody(new NakadiClient("clientId", scopes));
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

    private void prepareScopeRead() throws NakadiException, InvalidCursorException {
        EVENT_TYPE.setReadScopes(SCOPE_READ);
        final EventConsumer eventConsumerMock = mock(EventConsumer.class);
        when(eventTypeRepository.findByName(TEST_EVENT_TYPE_NAME)).thenReturn(EVENT_TYPE);
        when(topicRepositoryMock.createEventConsumer(
                eq(KAFKA_CLIENT_ID), eq(ImmutableList.of(new NakadiCursor(fakeTimeline, "0", "0")))))
                .thenReturn(eventConsumerMock);
    }

    protected String responseToString(final StreamingResponseBody responseBody) throws IOException {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        responseBody.writeTo(out);
        return out.toString();
    }

    protected StreamingResponseBody createStreamingResponseBody() throws IOException {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, 0, 0, 0, 0, 0, null, requestMock, responseMock,
                FULL_ACCESS_CLIENT);
    }

    private StreamingResponseBody createStreamingResponseBody(final Client client) throws Exception {
        return controller.streamEvents(
                TEST_EVENT_TYPE_NAME, 1, 2, 3, 4, 5, "[{\"partition\":\"0\",\"offset\":\"000000000000000000\"}]",
                requestMock, responseMock, client);
    }

    private StreamingResponseBody createStreamingResponseBody(final String cursorsStr) throws Exception {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, 1, 2, 3, 4, 5, cursorsStr,
                requestMock, responseMock, FULL_ACCESS_CLIENT);
    }

    private StreamingResponseBody createStreamingResponseBody(final Integer batchLimit,
                                                              final Integer streamLimit,
                                                              final Integer batchTimeout,
                                                              final Integer streamTimeout,
                                                              final Integer streamKeepAliveLimit,
                                                              final String cursorsStr) throws IOException {
        return controller.streamEvents(TEST_EVENT_TYPE_NAME, batchLimit, streamLimit, batchTimeout, streamTimeout,
                streamKeepAliveLimit, cursorsStr, requestMock, responseMock, FULL_ACCESS_CLIENT);
    }

}
