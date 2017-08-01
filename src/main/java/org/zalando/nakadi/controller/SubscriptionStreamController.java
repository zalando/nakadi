package org.zalando.nakadi.controller;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import static org.zalando.nakadi.metrics.MetricUtils.metricNameForSubscription;
import org.zalando.nakadi.repository.db.SubscriptionDbRepository;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.BlacklistService;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.SubscriptionStreamer;
import org.zalando.nakadi.service.subscription.SubscriptionStreamerFactory;
import org.zalando.nakadi.util.FeatureToggleService;
import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;
import org.zalando.problem.Problem;

@RestController
public class SubscriptionStreamController {
    public static final String CONSUMERS_COUNT_METRIC_NAME = "consumers";
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStreamController.class);

    private final SubscriptionStreamerFactory subscriptionStreamerFactory;
    private final FeatureToggleService featureToggleService;
    private final ObjectMapper jsonMapper;
    private final ClosedConnectionsCrutch closedConnectionsCrutch;
    private final NakadiSettings nakadiSettings;
    private final BlacklistService blacklistService;
    private final MetricRegistry metricRegistry;
    private final SubscriptionDbRepository subscriptionDbRepository;

    @Autowired
    public SubscriptionStreamController(final SubscriptionStreamerFactory subscriptionStreamerFactory,
                                        final FeatureToggleService featureToggleService,
                                        final ObjectMapper objectMapper,
                                        final ClosedConnectionsCrutch closedConnectionsCrutch,
                                        final NakadiSettings nakadiSettings,
                                        final BlacklistService blacklistService,
                                        @Qualifier("perPathMetricRegistry") final MetricRegistry metricRegistry,
                                        final SubscriptionDbRepository subscriptionDbRepository) {
        this.subscriptionStreamerFactory = subscriptionStreamerFactory;
        this.featureToggleService = featureToggleService;
        this.jsonMapper = objectMapper;
        this.closedConnectionsCrutch = closedConnectionsCrutch;
        this.nakadiSettings = nakadiSettings;
        this.blacklistService = blacklistService;
        this.metricRegistry = metricRegistry;
        this.subscriptionDbRepository = subscriptionDbRepository;
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
            LOG.warn("Exception occurred while streaming", ex);
            if (!headersSent) {
                headersSent = true;
                try {
                    if (ex instanceof AccessDeniedException) {
                        writeProblemResponse(response, out, Problem.valueOf(Response.Status.FORBIDDEN,
                                ((AccessDeniedException) ex).explain()));
                    }
                    if (ex instanceof NakadiException) {
                        writeProblemResponse(response, out, ((NakadiException) ex).asProblem());
                    } else {
                        writeProblemResponse(response, out, Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
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

    @RequestMapping(value = "/subscriptions/{subscription_id}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEvents(
            @PathVariable("subscription_id") final String subscriptionId,
            @RequestParam(value = "max_uncommitted_events", required = false, defaultValue = "10")
            final int maxUncommittedSize,
            @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final int batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Long streamLimit,
            @RequestParam(value = "batch_flush_timeout", required = false, defaultValue = "30") final int batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Long streamTimeout,
            @Nullable
            @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer streamKeepAliveLimit,
            final HttpServletRequest request, final HttpServletResponse response, final Client client)
            throws IOException {

        return outputStream -> {
            if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
                response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
                return;
            }

            final String metricName = metricNameForSubscription(subscriptionId, CONSUMERS_COUNT_METRIC_NAME);
            final Counter consumerCounter = metricRegistry.counter(metricName);
            consumerCounter.inc();

            final AtomicBoolean connectionReady = closedConnectionsCrutch.listenForConnectionClose(request);

            SubscriptionStreamer streamer = null;
            final SubscriptionOutputImpl output = new SubscriptionOutputImpl(response, outputStream);
            try {
                if (blacklistService.isSubscriptionConsumptionBlocked(subscriptionId, client.getClientId())) {
                    writeProblemResponse(response, outputStream,
                            Problem.valueOf(Response.Status.FORBIDDEN, "Application or event type is blocked"));
                    return;
                }

                final StreamParameters streamParameters = StreamParameters.of(batchLimit, streamLimit, batchTimeout,
                        streamTimeout, streamKeepAliveLimit, maxUncommittedSize,
                        nakadiSettings.getDefaultCommitTimeoutSeconds(), client.getClientId());
                final Subscription subscription = subscriptionDbRepository.getSubscription(subscriptionId);

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
