package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.ShutdownHooks;
import org.zalando.nakadi.cache.SubscriptionCache;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ConflictException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidStreamParametersException;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionPartitionConflictException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.EventStreamChecks;
import org.zalando.nakadi.service.SubscriptionValidationService;
import org.zalando.nakadi.service.TracingService;
import org.zalando.nakadi.service.subscription.StreamContentType;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.SubscriptionStreamer;
import org.zalando.nakadi.service.subscription.SubscriptionStreamerFactory;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.util.MDCUtils;
import org.zalando.nakadi.view.UserStreamParameters;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.zalando.nakadi.metrics.MetricUtils.metricNameForSubscription;
import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.PRECONDITION_FAILED;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
public class SubscriptionStreamController {
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStreamController.class);
    public static final MediaType BINARY_MEDIA_TYPE = new MediaType("application", "avro-binary");

    private final SubscriptionStreamerFactory subscriptionStreamerFactory;
    private final ObjectMapper jsonMapper;
    private final NakadiSettings nakadiSettings;
    private final EventStreamChecks eventStreamChecks;
    private final MetricRegistry metricRegistry;
    private final SubscriptionCache subscriptionCache;
    private final SubscriptionValidationService subscriptionValidationService;
    private final ShutdownHooks shutdownHooks;

    @Autowired
    public SubscriptionStreamController(final SubscriptionStreamerFactory subscriptionStreamerFactory,
                                        final ObjectMapper objectMapper,
                                        final NakadiSettings nakadiSettings,
                                        final EventStreamChecks eventStreamChecks,
                                        @Qualifier("perPathMetricRegistry") final MetricRegistry metricRegistry,
                                        final SubscriptionCache subscriptionCache,
                                        final SubscriptionValidationService subscriptionValidationService,
                                        final ShutdownHooks shutdownHooks) {
        this.subscriptionStreamerFactory = subscriptionStreamerFactory;
        this.jsonMapper = objectMapper;
        this.nakadiSettings = nakadiSettings;
        this.eventStreamChecks = eventStreamChecks;
        this.metricRegistry = metricRegistry;
        this.subscriptionCache = subscriptionCache;
        this.subscriptionValidationService = subscriptionValidationService;
        this.shutdownHooks = shutdownHooks;
    }

    class SubscriptionOutputImpl implements SubscriptionOutput {
        private boolean headersSent;
        private final HttpServletResponse response;
        private final OutputStream out;
        private final Map<Class, Function<Exception, Problem>> exceptionProblem;

        SubscriptionOutputImpl(final HttpServletResponse response, final OutputStream out) {
            this.response = response;
            this.out = out;
            this.headersSent = false;
            this.exceptionProblem = new HashMap<>();
            assignExceptionProblem();
        }

        private void assignExceptionProblem() {
            this.exceptionProblem.put(AccessDeniedException.class,
                    (ex) -> Problem.valueOf(FORBIDDEN, ((AccessDeniedException) ex).explain()));
            this.exceptionProblem.put(SubscriptionPartitionConflictException.class,
                    (ex) -> Problem.valueOf(CONFLICT, ex.getMessage()));
            this.exceptionProblem.put(InvalidStreamParametersException.class,
                    (ex) -> Problem.valueOf(UNPROCESSABLE_ENTITY, ex.getMessage()));
            this.exceptionProblem.put(NoSuchSubscriptionException.class,
                    (ex) -> Problem.valueOf(NOT_FOUND, ex.getMessage()));
            this.exceptionProblem.put(InternalNakadiException.class,
                    (ex) -> Problem.valueOf(INTERNAL_SERVER_ERROR, ex.getMessage()));
            this.exceptionProblem.put(NoStreamingSlotsAvailable.class,
                    (ex) -> Problem.valueOf(CONFLICT, ex.getMessage()));
            this.exceptionProblem.put(ConflictException.class,
                    (ex) -> Problem.valueOf(CONFLICT, ex.getMessage()));
            this.exceptionProblem.put(InvalidCursorException.class,
                    (ex) -> Problem.valueOf(PRECONDITION_FAILED, ex.getMessage()));
        }

        @Override
        public void onInitialized(final String sessionId) throws IOException {
            if (!headersSent) {
                headersSent = true;
                response.setStatus(HttpStatus.OK.value());
                response.setContentType("application/x-json-stream");
                response.setHeader("X-Nakadi-StreamId", sessionId);
                out.flush();
            }
        }

        @Override
        public void onException(final Exception ex) {
            //
            // Here we are trying to avoid spamming the logs with stacktraces for very common errors like "no free
            // slots" or "access denied".
            //
            // FIXME: maybe these should not be exceptions in the first place?..
            //
            if (ex instanceof InternalNakadiException) {
                LOG.error("Internal error occurred while streaming", ex);
            } else {
                LOG.info("Exception occurred while streaming: {}: {}", ex.getClass().getName(), ex.getMessage());
            }
            if (!headersSent) {
                headersSent = true;
                try {
                    writeProblemResponse(response, out, exceptionProblem.getOrDefault(ex.getClass(),
                            (e) -> Problem.valueOf(SERVICE_UNAVAILABLE, "Failed to continue streaming")).apply(ex));
                    out.flush();
                } catch (final IOException e) {
                    LOG.error("Failed to write exception to response", e);
                }
            } else {
                LOG.warn("Response was already sent, cannot report error to the client");
            }
        }

        @Override
        public OutputStream getOutputStream() {
            return this.out;
        }

    }

    @RequestMapping(value = "/subscriptions/{subscription_id}/events", method = RequestMethod.POST)
    public StreamingResponseBody streamEvents(
            @PathVariable("subscription_id") final String subscriptionId,
            @Valid @RequestBody final UserStreamParameters userParameters,
            @RequestHeader(name = "Accept", required = false,
                    defaultValue = "application/x-json-stream") final String acceptHeader,
            final HttpServletResponse response,
            final Client client) {

        final StreamParameters streamParameters = StreamParameters.of(userParameters,
                nakadiSettings.getMaxCommitTimeout(), client);
        return stream(subscriptionId, response, client, streamParameters, mapToStreamContentType(acceptHeader));
    }

    @RequestMapping(value = "/subscriptions/{subscription_id}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("subscription_id") final String subscriptionId,
            @Nullable @RequestParam(value = "max_uncommitted_events", required = false) final Integer
                    maxUncommittedEvents,
            @Nullable @RequestParam(value = "batch_limit", required = false) final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Long streamLimit,
            @Nullable @RequestParam(value = "batch_timespan", required = false) final Long batchTimespan,
            @Nullable @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Long streamTimeout,
            @Nullable @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer
                    streamKeepAliveLimit,
            @Nullable @RequestParam(value = "commit_timeout", required = false) final Long commitTimeout,
            @RequestHeader(name = "Accept", required = false,
                    defaultValue = "application/x-json-stream") final String acceptHeader,
            final HttpServletResponse response, final Client client) {

        final UserStreamParameters userParameters = new UserStreamParameters(batchLimit, streamLimit, batchTimespan,
                batchTimeout, streamTimeout, streamKeepAliveLimit, maxUncommittedEvents, ImmutableList.of(),
                commitTimeout);

        final StreamParameters streamParameters = StreamParameters.of(userParameters,
                nakadiSettings.getMaxCommitTimeout(), client);

        return stream(subscriptionId, response, client, streamParameters, mapToStreamContentType(acceptHeader));
    }

    private StreamContentType mapToStreamContentType(final String acceptHeader) {
        final List<MediaType> mediaTypes = MediaType.parseMediaTypes(acceptHeader);
        if (mediaTypes.size() == 1 &&
                BINARY_MEDIA_TYPE.equalsTypeAndSubtype(mediaTypes.get(0))) {
            return StreamContentType.BINARY;
        } else {
            return StreamContentType.JSON;
        }
    }

    private StreamingResponseBody stream(final String subscriptionId,
                                         final HttpServletResponse response,
                                         final Client client,
                                         final StreamParameters streamParameters,
                                         final StreamContentType streamContentType) {
        final Session session = Session.generate(1, streamParameters.getPartitions());
        try (MDCUtils.CloseableNoEx ignore1 = MDCUtils.withSubscriptionIdStreamId(subscriptionId, session.getId())) {
            TracingService.setOperationName("stream_events")
                    .setTag("subscription.id", subscriptionId);

            final Span requestSpan = TracingService.getActiveSpan();
            final MDCUtils.Context loggingContext = MDCUtils.getContext();

            return outputStream -> {
                try (MDCUtils.CloseableNoEx ignore2 = MDCUtils.withContext(loggingContext)) {
                    final String metricName = metricNameForSubscription(subscriptionId, CONSUMERS_COUNT_METRIC_NAME);
                    final Counter consumerCounter = metricRegistry.counter(metricName);
                    consumerCounter.inc();

                    SubscriptionStreamer streamer = null;
                    final SubscriptionOutputImpl output = new SubscriptionOutputImpl(response, outputStream);

                    try {
                        if (eventStreamChecks.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                            writeProblemResponse(response, outputStream,
                                    Problem.valueOf(FORBIDDEN, "Application or event type is blocked"));
                            return;
                        }
                        final Subscription subscription = subscriptionCache.getSubscription(subscriptionId);
                        subscriptionValidationService.validatePartitionsToStream(subscription,
                                streamParameters.getPartitions());

                        streamer = subscriptionStreamerFactory.build(subscription, streamParameters,
                                session, output, streamContentType);

                        final Tracer.SpanBuilder spanBuilder =
                                TracingService.buildNewFollowerSpan(
                                        "streaming_async", requestSpan.context())
                                        .withTag("client", client.getClientId())
                                        .withTag("session.id", session.getId())
                                        .withTag("subscription.id", subscriptionId);
                        try (
                                Closeable ignore3 = TracingService.withActiveSpan(spanBuilder);
                                Closeable ignore4 = shutdownHooks.addHook(streamer::terminateStream)
                        ) {
                            streamer.stream();
                        }
                    } catch (final InterruptedException ex) {
                        LOG.warn("Interrupted while streaming with " + streamer, ex);
                        Thread.currentThread().interrupt();
                    } catch (final RuntimeException e) {
                        output.onException(e);
                    } finally {
                        consumerCounter.dec();
                        outputStream.close();
                    }
                }
            };
        }
    }

    private void writeProblemResponse(final HttpServletResponse response,
                                      final OutputStream outputStream,
                                      final Problem problem) throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        response.setContentType("application/problem+json");
        jsonMapper.writer().writeValue(outputStream, problem);
    }
}
