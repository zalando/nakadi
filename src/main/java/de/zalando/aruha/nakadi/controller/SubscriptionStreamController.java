package de.zalando.aruha.nakadi.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.exceptions.NoSuchSubscriptionException;
import de.zalando.aruha.nakadi.service.subscription.StreamParameters;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionOutput;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionStreamer;
import de.zalando.aruha.nakadi.service.subscription.SubscriptionStreamerFactory;
import java.io.IOException;
import java.io.OutputStream;
import javax.servlet.http.HttpServletResponse;
import javax.ws.rs.core.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;
import org.zalando.problem.Problem;

@RestController
public class SubscriptionStreamController {
    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionStreamController.class);

    private final SubscriptionStreamerFactory subscriptionStreamerFactory;
    private final ObjectMapper jsonMapper;

    @Autowired
    public SubscriptionStreamController(final SubscriptionStreamerFactory subscriptionStreamerFactory, final ObjectMapper objectMapper) {
        this.subscriptionStreamerFactory = subscriptionStreamerFactory;
        this.jsonMapper = objectMapper;
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
        public void onInitialized() throws IOException {
            if (!headersSent) {
                headersSent = true;
                response.setStatus(HttpStatus.OK.value());
                response.setContentType("application/x-json-stream");
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
                        writeProblemResponse(Response.Status.BAD_REQUEST, ((NakadiException) ex).getProblemMessage());
                    } else {
                        writeProblemResponse(Response.Status.SERVICE_UNAVAILABLE, "Failed to continue streaming");
                    }
                } catch (final IOException e) {
                    LOG.error("Failed to write exception to response", e);
                }
            } else {
                LOG.warn("Exception found while streaming, but no data could be provided to client", ex);
            }
        }

        void writeProblemResponse(final Response.StatusType statusCode, final String message) throws IOException {
            response.setStatus(statusCode.getStatusCode());
            response.setContentType("application/problem+json");
            jsonMapper.writer().writeValue(out, Problem.valueOf(statusCode, message));
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
            @RequestParam(value = "window_size", required = false, defaultValue = "100") final int windowSize,
            @RequestParam(value = "commit_timeout", required = false, defaultValue = "30") final int commitTimeout,
            @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final int batchLimit,
            @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @RequestParam(value = "batch_flush_timeout", required = false, defaultValue = "30") final int batchTimeout,
            @RequestParam(value = "stream_timeout", required = false) final Long streamTimeout,
            @RequestParam(value = "stream_keep_alive_limit", required = false) final Integer streamKeepAliveLimit,
            final NativeWebRequest request, final HttpServletResponse response) throws IOException {

        return outputStream -> {
            SubscriptionStreamer streamer = null;
            final SubscriptionOutputImpl output = new SubscriptionOutputImpl(response, outputStream);
            try {
                streamer = subscriptionStreamerFactory.build(
                        subscriptionId,
                        StreamParameters.of(batchLimit, streamLimit, batchTimeout, streamTimeout, streamKeepAliveLimit, windowSize, commitTimeout),
                        output);
                streamer.stream();
            } catch (final NoSuchSubscriptionException e) {
                output.onException(e);
            } catch (final InterruptedException ex) {
                LOG.warn("Interrupted while streaming with " + streamer, ex);
                Thread.currentThread().interrupt();
            } finally {
                outputStream.close();
            }
        };
    }
}
