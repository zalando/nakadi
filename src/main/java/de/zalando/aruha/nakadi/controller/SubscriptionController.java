package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.domain.Subscription;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.SubscriptionRepository;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.service.EventStreamManager;
import de.zalando.aruha.nakadi.utils.FlushableGZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

import static java.util.Optional.ofNullable;

@RestController
@RequestMapping(value = "/subscriptions")
public class SubscriptionController {

    private static final Logger LOG = LoggerFactory.getLogger(SubscriptionController.class);

    @Autowired
    private TopicRepository topicRepository;

    @Autowired
    private SubscriptionRepository subscriptionRepository;

    @Autowired
    private EventStreamManager eventStreamManager;

    @Timed(name = "commit_offsets", absolute = true)
    @RequestMapping(value = "/{subscription}/cursors", method = RequestMethod.POST)
    public ResponseEntity commitOffsets(@PathVariable("subscription") final String subscriptionId,
                                        @RequestBody final List<Cursor> cursors) throws InterruptedException, NakadiException {
        try {
            final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);
            cursors.stream().forEach(cursor ->
                    subscription.updateCursor(cursor.getTopic(), cursor.getPartition(), cursor.getOffset()));
            subscriptionRepository.saveSubscription(subscription);
        } catch (Exception e) {
            LOG.error("Error during offsets commit", e.getCause());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new Problem("Commit error"));
        }
        return ResponseEntity.ok().build();
    }

    @Timed(name = "stream_events_for_subscription", absolute = true)
    @RequestMapping(value = "/{subscription}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEventsForSubscription(@PathVariable("subscription") final String subscriptionId,
            @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final Integer batchLimit,
            @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
            @RequestParam(value = "batch_keep_alive_limit", required = false) final Integer batchKeepAliveLimit,
            final HttpServletRequest request, final HttpServletResponse response) throws IOException {

        return outputStream -> {
            EventStream eventStream = null;
            try {
                response.setStatus(HttpStatus.OK.value());

                final String acceptEncoding = request.getHeader("Accept-Encoding");
                final boolean gzipEnabled = acceptEncoding != null && acceptEncoding.contains("gzip");
                final OutputStream output = gzipEnabled ? new FlushableGZIPOutputStream(outputStream) : outputStream;

                if (gzipEnabled) {
                    response.addHeader("Content-Encoding", "gzip");
                }

                final Subscription subscription = subscriptionRepository.getSubscription(subscriptionId);

                final EventConsumer eventConsumer = topicRepository.createEventConsumer();

                final EventStreamConfig streamConfig = EventStreamConfig
                        .builder()
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(ofNullable(streamLimit))
                        .withBatchTimeout(ofNullable(batchTimeout))
                        .withStreamTimeout(ofNullable(streamTimeout))
                        .withBatchKeepAliveLimit(ofNullable(batchKeepAliveLimit))
                        .build();

                eventStream = new EventStream(eventConsumer, output, streamConfig);
                eventStream.setSubscriptionId(subscriptionId);

                final String newClientId = subscriptionRepository.generateNewClientId(subscription);
                eventStream.setClientId(newClientId);
                eventStreamManager.addEventStream(eventStream);
                eventStream.streamEvents();

                if (gzipEnabled) {
                    output.close();
                }
            }
            catch (Exception e) {
                e.printStackTrace();
                // shit
            }
            finally {
                eventStreamManager.removeEventStream(eventStream);
                outputStream.flush();
                outputStream.close();
            }
        };
    }

}
