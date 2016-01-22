package de.zalando.aruha.nakadi.controller;

import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;

import java.io.IOException;
import java.io.OutputStream;

import java.util.List;
import java.util.function.Predicate;

import javax.annotation.Nullable;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.NakadiRuntimeException;
import de.zalando.aruha.nakadi.domain.Cursor;
import de.zalando.aruha.nakadi.domain.TopicPartitionOffsets;
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

import com.codahale.metrics.annotation.Timed;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
import de.zalando.aruha.nakadi.utils.FlushableGZIPOutputStream;

@RestController
@RequestMapping(value = "/topics")
public class TopicsController {

    private static final Logger LOG = LoggerFactory.getLogger(TopicsController.class);

    @Autowired
    private TopicRepository topicRepository;

    @Autowired
    private ObjectMapper jsonMapper;

    @Timed(name = "get_topics", absolute = true)
    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> listTopics() {
        try {
            return ok().body(topicRepository.listTopics());
        } catch (final NakadiException e) {
            return status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getProblemMessage());
        }
    }

    @Timed(name = "get_partitions", absolute = true)
    @RequestMapping(value = "/{topicId}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("topicId") final String topicId) {
        try {
            return ok().body(topicRepository.listPartitionsOffsets(topicId));
        } catch (final NakadiRuntimeException e) {
            return status(HttpStatus.SERVICE_UNAVAILABLE).body(e.asProblem());
        }
    }

    @Timed(name = "get_partition", absolute = true)
    @RequestMapping(value = "/{topicId}/partitions/{partitionId}", method = RequestMethod.GET)
    public TopicPartitionOffsets getPartition(@PathVariable("topicId") final String topicId) {
        throw new UnsupportedOperationException();
    }

    @Timed(name = "post_event_to_partition", absolute = true)
    @RequestMapping(value = "/{topicId}/partitions/{partitionId}/events", method = RequestMethod.POST)
    public ResponseEntity<?> postEventToPartition(@PathVariable("topicId") final String topicId,
            @PathVariable("partitionId") final String partitionId, @RequestBody final String messagePayload) {
        LOG.trace("Event received: {}, {}, {}", topicId, partitionId, messagePayload);
        try {
            topicRepository.postEvent(topicId, partitionId, messagePayload);
            return status(HttpStatus.CREATED).build();
        } catch (final NakadiException e) {
            LOG.error("error posting to partition", e);
            return status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getProblemMessage());
        }
    }

    @Timed(name = "stream_events_from_partition", absolute = true)
    @RequestMapping(value = "/{topic}/partitions/{partition}/events", method = RequestMethod.GET)
    public StreamingResponseBody streamEventsFromPartition(@PathVariable("topic") final String topic,
            @PathVariable("partition") final String partition,
            @RequestParam("start_from") final String startFrom,
            @Nullable @RequestParam(value = "batch_limit", required = false, defaultValue = "1") final Integer batchLimit,
            @Nullable @RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
            @Nullable @RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
            @Nullable @RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
            @Nullable @RequestParam(value = "batch_keep_alive_limit", required = false) final Integer batchKeepAliveLimit,
            final HttpServletRequest request, final HttpServletResponse response) throws IOException {

        return
            outputStream -> {
            try {

                // check if topic exists
                final boolean topicExists = topicRepository.listTopics().stream().anyMatch(t ->
                            topic.equals(t.getName()));
                if (!topicExists) {
                    writeProblemResponse(response, outputStream, HttpStatus.NOT_FOUND.value(),
                        new Problem("topic not found"));
                }

                // check if partition exists
                final List<TopicPartitionOffsets> topicPartitions = topicRepository.listPartitionsOffsets(topic);
                final Predicate<TopicPartitionOffsets> tpPredicate = tp ->
                        topic.equals(tp.getTopicId()) && partition.equals(tp.getPartitionId());
                final boolean partitionExists = topicPartitions.stream().anyMatch(tpPredicate);
                if (!partitionExists) {
                    writeProblemResponse(response, outputStream, HttpStatus.NOT_FOUND.value(),
                        new Problem("partition not found"));
                }

                // check if offset is correct
                final boolean offsetCorrect = topicPartitions.stream().filter(tpPredicate).findFirst().map(tp ->
                        topicRepository.validateOffset(startFrom,
                                tp.getNewestAvailableOffset(),
                                tp.getOldestAvailableOffset())).orElse(false);
                if (!offsetCorrect) {
                    writeProblemResponse(response, outputStream, HttpStatus.BAD_REQUEST.value(),
                        new Problem("start_from is invalid"));
                }

                final EventStreamConfig streamConfig = EventStreamConfig
                        .builder()
                        .withTopic(topic)
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(streamLimit)
                        .withBatchTimeout(batchTimeout)
                        .withStreamTimeout(streamTimeout)
                        .withBatchKeepAliveLimit(batchKeepAliveLimit)
                        .build();

                response.setStatus(HttpStatus.OK.value());

                final String acceptEncoding = request.getHeader("Accept-Encoding");
                final boolean gzipEnabled = acceptEncoding != null && acceptEncoding.contains("gzip");
                final OutputStream output = gzipEnabled ? new FlushableGZIPOutputStream(outputStream) : outputStream;

                if (gzipEnabled) {
                    response.addHeader("Content-Encoding", "gzip");
                }

                final ImmutableList<Cursor> cursors = ImmutableList.of(new Cursor(topic, partition, startFrom));
                final EventConsumer eventConsumer = topicRepository.createEventConsumer(cursors);
                final EventStream eventStream = new EventStream(eventConsumer, output, streamConfig, cursors);
                eventStream.streamEvents();

                if (gzipEnabled) {
                    output.close();
                }

            } catch (NakadiException e) {
                writeProblemResponse(response, outputStream, HttpStatus.SERVICE_UNAVAILABLE.value(), e.asProblem());
            } finally {
                outputStream.flush();
                outputStream.close();
            }
        };
    }

    private void writeProblemResponse(final HttpServletResponse response, final OutputStream outputStream,
            final int statusCode, final Problem problem) throws IOException {
        response.setStatus(statusCode);
        jsonMapper.writer().writeValue(outputStream, problem);
    }

}
