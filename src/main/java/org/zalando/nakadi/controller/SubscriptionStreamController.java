package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.NoStreamingSlotsAvailable;
import org.zalando.nakadi.exceptions.runtime.NoSuchSubscriptionException;
import org.zalando.nakadi.exceptions.runtime.SubscriptionPartitionConflictException;
import org.zalando.nakadi.exceptions.runtime.WrongStreamParametersException;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.SubscriptionStreamer;
import org.zalando.nakadi.service.subscription.SubscriptionStreamerFactory;
import org.zalando.nakadi.service.subscription.SubscriptionValidationService;
import org.zalando.nakadi.util.FlowIdUtils;
import org.zalando.nakadi.view.UserStreamParameters;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zalando.nakadi.metrics.MetricUtils.metricNameForSubscription;
import static org.zalando.problem.Status.CONFLICT;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.INTERNAL_SERVER_ERROR;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
public class SubscriptionStreamController {
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStreamController.class);

    private final SubscriptionStreamerFactory subscriptionStreamerFactory;
    private final ObjectMapper jsonMapper;
    private final ClosedConnectionsCrutch closedConnectionsCrutch;
    private final NakadiSettings nakadiSettings;
    private final BlacklistService blacklistService;
    private final MetricRegistry metricRegistry;
    private final SubscriptionDbRepository subscriptionDbRepository;
    private final SubscriptionValidationService subscriptionValidationService;

    @Autowired
    public SubscriptionStreamController(final SubscriptionStreamerFactory subscriptionStreamerFactory,
                                        final ObjectMapper objectMapper,
                                        final ClosedConnectionsCrutch closedConnectionsCrutch,
                                        final NakadiSettings nakadiSettings,
                                        final BlacklistService blacklistService,
                                        @Qualifier("perPathMetricRegistry") final MetricRegistry metricRegistry,
                                        final SubscriptionDbRepository subscriptionDbRepository,
                                        final SubscriptionValidationService subscriptionValidationService) {
        this.subscriptionStreamerFactory = subscriptionStreamerFactory;
        this.jsonMapper = objectMapper;
        this.closedConnectionsCrutch = closedConnectionsCrutch;
        this.nakadiSettings = nakadiSettings;
        this.blacklistService = blacklistService;
        this.metricRegistry = metricRegistry;
        this.subscriptionDbRepository = subscriptionDbRepository;
        this.subscriptionValidationService = subscriptionValidationService;
    }

    private class SubscriptionOutputImpl implements SubscriptionOutput {
        private boolean headersSent;
        private final HttpServletResponse response;
        private final OutputStream out;

        SubscriptionOutputImpl(final HttpServletResponse response, final OutputStream out) {
            this.response = response;
            this.out = out;
            this.headersSent = false;
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
            LOG.warn("Exception occurred while streaming: {}", ex.getMessage());
            if (!headersSent) {
                headersSent = true;
                try {
                    if (ex instanceof AccessDeniedException) {
                        writeProblemResponse(response, out, Problem.valueOf(FORBIDDEN,
                                ((AccessDeniedException) ex).explain()));
                    } else if (ex instanceof SubscriptionPartitionConflictException) {
                        writeProblemResponse(response, out, Problem.valueOf(CONFLICT,
                                ex.getMessage()));
                    } else if (ex instanceof WrongStreamParametersException) {
                        writeProblemResponse(response, out, Problem.valueOf(UNPROCESSABLE_ENTITY,
                                ex.getMessage()));
                    } else if (ex instanceof NoSuchSubscriptionException) {
                        writeProblemResponse(response, out, Problem.valueOf(NOT_FOUND,
                                ex.getMessage()));
                    } else if (ex instanceof InternalNakadiException) {
                        writeProblemResponse(response, out, Problem.valueOf(INTERNAL_SERVER_ERROR, ex.getMessage()));
                    } else if (ex instanceof NoStreamingSlotsAvailable) {
                        writeProblemResponse(response, out, Problem.valueOf(CONFLICT, ex.getMessage()));
                    } else {
                        writeProblemResponse(response, out, Problem.valueOf(SERVICE_UNAVAILABLE,
                                "Failed to continue streaming"));
                    }
                    out.flush();
                } catch (final IOException e) {
                    LOG.error("Failed to write exception to response", e);
                }
            } else {
                LOG.warn("Exception found while streaming, but no data could be provided to client", ex);
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
            final HttpServletRequest request,
            final HttpServletResponse response,
            final Client client) {

        final StreamParameters streamParameters = StreamParameters.of(userParameters,
                nakadiSettings.getDefaultCommitTimeoutSeconds(), client);

        return stream(subscriptionId, request, response, client, streamParameters);
    }

    @RequestMapping(value = "/subscriptions/{subscription_id}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("subscription_id") final String subscriptionId,
            @Nullable @RequestParam(value = "max_uncommitted_events", required = false) final Integer
                    maxUncommittedEvents,
            @Nullable @RequestParam(value = "batch_limit", required = false) final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Long streamLimit,
            @Nullable @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Long streamTimeout,
            @Nullable @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer
                    streamKeepAliveLimit,
            final HttpServletRequest request, final HttpServletResponse response, final Client client) {

        final UserStreamParameters userParameters = new UserStreamParameters(batchLimit, streamLimit, batchTimeout,
                streamTimeout, streamKeepAliveLimit, maxUncommittedEvents, ImmutableList.of());

        final StreamParameters streamParameters = StreamParameters.of(userParameters,
                nakadiSettings.getDefaultCommitTimeoutSeconds(), client);

        return stream(subscriptionId, request, response, client, streamParameters);
    }

    private StreamingResponseBody stream(final String subscriptionId,
                                         final HttpServletRequest request,
                                         final HttpServletResponse response,
                                         final Client client,
                                         final StreamParameters streamParameters) {
        final String flowId = FlowIdUtils.peek();

        return outputStream -> {
            FlowIdUtils.push(flowId);

            final String metricName = metricNameForSubscription(subscriptionId, CONSUMERS_COUNT_METRIC_NAME);
            final Counter consumerCounter = metricRegistry.counter(metricName);
            consumerCounter.inc();

            final AtomicBoolean connectionReady = closedConnectionsCrutch.listenForConnectionClose(request);

            SubscriptionStreamer streamer = null;
            final SubscriptionOutputImpl output = new SubscriptionOutputImpl(response, outputStream);
            try {
                if (blacklistService.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                    writeProblemResponse(response, outputStream,
                            Problem.valueOf(FORBIDDEN, "Application or event type is blocked"));
                    return;
                }

                final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);
                subscriptionValidationService.validatePartitionsToStream(subscription,
                        streamParameters.getPartitions());

                streamer = subscriptionStreamerFactory.build(subscription, streamParameters, output,
                        connectionReady, blacklistService);

                streamer.stream();
            } catch (final InterruptedException ex) {
                LOG.warn("Interrupted while streaming with " + streamer, ex);
                Thread.currentThread().interrupt();
            } catch (final Exception e) {
                output.onException(e);
            } finally {
                consumerCounter.dec();
                outputStream.close();
            }
        };
    }

    private void writeProblemResponse(final HttpServletResponse response,
                                      final OutputStream outputStream,
                                      final Problem problem) throws IOException {
        response.setStatus(problem.getStatus().getStatusCode());
        response.setContentType("application/problem+json");
        jsonMapper.writer().writeValue(outputStream, problem);
    }
}
