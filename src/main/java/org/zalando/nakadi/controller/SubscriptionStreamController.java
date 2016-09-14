package org.zalando.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.service.ClosedConnectionsCrutch;
import org.zalando.nakadi.service.subscription.StreamParameters;
import org.zalando.nakadi.service.subscription.SubscriptionOutput;
import org.zalando.nakadi.service.subscription.SubscriptionStreamer;
import org.zalando.nakadi.service.subscription.SubscriptionStreamerFactory;
import org.zalando.nakadi.util.FeatureToggleService;
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.zalando.nakadi.util.FeatureToggleService.Feature.HIGH_LEVEL_API;

@RestController
public class SubscriptionStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStreamController.class);

    private final SubscriptionStreamerFactory subscriptionStreamerFactory;
    private final FeatureToggleService featureToggleService;
    private final ObjectMapper jsonMapper;
    private final ClosedConnectionsCrutch closedConnectionsCrutch;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public SubscriptionStreamController(final SubscriptionStreamerFactory subscriptionStreamerFactory,
                                        final FeatureToggleService featureToggleService,
                                        final ObjectMapper objectMapper,
                                        final ClosedConnectionsCrutch closedConnectionsCrutch,
                                        final NakadiSettings nakadiSettings) {
        this.subscriptionStreamerFactory = subscriptionStreamerFactory;
        this.featureToggleService = featureToggleService;
        this.jsonMapper = objectMapper;
        this.closedConnectionsCrutch = closedConnectionsCrutch;
        this.nakadiSettings = nakadiSettings;
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
                    if (ex instanceof NakadiException) {
                        writeProblemResponse(((NakadiException) ex).asProblem());
                    } else {
                        writeProblemResponse(Problem.valueOf(Response.Status.SERVICE_UNAVAILABLE,
                                "Failed to continue streaming"));
                    }
                } catch (final IOException e) {
                    LOG.error("Failed to write exception to response", e);
                }
            } else {
                LOG.warn("Exception found while streaming, but no data could be provided to client", ex);
            }
        }

        void writeProblemResponse(final Problem problem) throws IOException {
            response.setStatus(problem.getStatus().getStatusCode());
            response.setContentType("application/problem+json");
            jsonMapper.writer().writeValue(out, problem);
        }

        @Override
        public void streamData(final byte[] data) throws IOException {
            headersSent = true;
            out.write(data);
            out.flush();
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
            final HttpServletRequest request, final HttpServletResponse response) throws IOException {

        return outputStream -> {

            if (!featureToggleService.isFeatureEnabled(HIGH_LEVEL_API)) {
                response.setStatus(HttpServletResponse.SC_NOT_IMPLEMENTED);
                return;
            }

            final AtomicBoolean connectionReady = closedConnectionsCrutch.listenForConnectionClose(request);

            SubscriptionStreamer streamer = null;
            final SubscriptionOutputImpl output = new SubscriptionOutputImpl(response, outputStream);
            try {
                final StreamParameters streamParameters = StreamParameters.of(batchLimit, streamLimit, batchTimeout,
                        streamTimeout, streamKeepAliveLimit, maxUncommittedSize,
                        nakadiSettings.getDefaultCommitTimeoutSeconds());
                streamer = subscriptionStreamerFactory.build(subscriptionId, streamParameters, output, connectionReady);
                streamer.stream();
            } catch (final InterruptedException ex) {
                LOG.warn("Interrupted while streaming with " + streamer, ex);
                Thread.currentThread().interrupt();
            } catch (final Exception e) {
                output.onException(e);
            } finally {
                outputStream.close();
            }
        };
    }
}
