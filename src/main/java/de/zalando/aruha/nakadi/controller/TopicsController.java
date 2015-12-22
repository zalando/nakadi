package de.zalando.aruha.nakadi.controller;

import static java.util.Optional.ofNullable;
import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import de.zalando.aruha.nakadi.repository.EventConsumer;
import de.zalando.aruha.nakadi.service.EventStream;
import de.zalando.aruha.nakadi.service.EventStreamConfig;
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

import com.codahale.metrics.annotation.Timed;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.OutputStream;

@RestController
@RequestMapping(value = "/topics")
public class TopicsController {

	private static final Logger LOG = LoggerFactory.getLogger(TopicsController.class);

	@Autowired
	private TopicRepository topicRepository;

    @Autowired
    private ObjectMapper jsonMapper;

	@Timed
	@RequestMapping(method = RequestMethod.GET)
	@ApiOperation("Lists all known topics")

	// FIXME: response for 200 does not match reality
	@ApiResponses({ @ApiResponse(code = 200, message = "Returns list of all topics", response = Topic.class),
			@ApiResponse(code = 401, message = "User not authenticated", response = Problem.class),
			@ApiResponse(code = 503, message = "Not available", response = Problem.class) })
	public ResponseEntity<?> listTopics() {
		try {
			return ok().body(topicRepository.listTopics());
		} catch (final NakadiException e) {
			return status(503).body(e.getProblemMessage());
		}
	}

	@Timed
	@RequestMapping(value = "/{topicId}/partitions", method = RequestMethod.GET)
	@ApiOperation("Lists the partitions for the given topic")
	@ApiResponses({ @ApiResponse(code = 200, message = "Returns list of all partitions for the given topic",
			response = TopicPartition.class),
		@ApiResponse(code = 401, message = "User not authenticated", response = Problem.class),
		@ApiResponse(code = 503, message = "Not available", response = Problem.class) })
	public ResponseEntity<?> listPartitions(
			@ApiParam(name = "topic", value = "Topic name", required = true) @PathVariable("topicId") final String topicId) {
		try {
			return ok().body(topicRepository.listPartitions(topicId));
		} catch (final NakadiException e) {
			return status(503).body(e.getProblemMessage());
		}
	}

	@Timed
	@RequestMapping(value = "/{topicId}/partitions/{partitionId}", method = RequestMethod.GET)
	public TopicPartition getPartition(@PathVariable("topicId") final String topicId) {
		throw new UnsupportedOperationException();
	}

	@Timed
	@RequestMapping(value = "/{topicId}/partitions/{partitionId}/events", method = RequestMethod.POST)
	@ApiOperation("Posts an event to the specified partition of this topic.")
	@ApiResponses({ @ApiResponse(code = 201, message = "Event submitted"),
		@ApiResponse(code = 401, message = "User not authenticated", response = Problem.class),
		@ApiResponse(code = 503, message = "Not available", response = Problem.class) })
	public ResponseEntity<?> postEventToPartition(
			@ApiParam(name = "topic", value = "Topic where to send events to", required = true) @PathVariable("topicId") final String topicId,
			@ApiParam(name = "partition", value = "Partition where to send events to", required = true) @PathVariable("partitionId") final String partitionId,
			@RequestBody final String messagePayload) {
		LOG.trace("Event received: {}, {}, {}", new Object[] { topicId, partitionId, messagePayload });
		try {
			topicRepository.postEvent(topicId, partitionId, messagePayload);
			return ok().build();
		} catch (final NakadiException e) {
			LOG.error("error posting to partition", e);
			return status(500).body(e.getProblemMessage());
		}
	}

	@Timed
	@RequestMapping(value = "/{topic}/partitions/{partition}/events", method = RequestMethod.GET)
	public StreamingResponseBody streamEventsFromPartition(
			@PathVariable("topic") final String topic,
			@PathVariable("partition") final String partition,
			@RequestParam("start_from") final String startFrom,
			@RequestParam(value = "batch_limit", required = false, defaultValue = "1") final Integer batchLimit,
			@RequestParam(value = "stream_limit", required = false) final Integer streamLimit,
			@RequestParam(value = "batch_flush_timeout", required = false) final Integer batchTimeout,
			@RequestParam(value = "stream_timeout", required = false) final Integer streamTimeout,
			@RequestParam(value = "batch_keep_alive_limit", required = false) final Integer batchKeepAliveLimit,
            final HttpServletRequest request,
            final HttpServletResponse response) throws IOException {

        return outputStream -> {
            try {
                // check if topic exists
                final boolean topicExists = topicRepository.listTopics().stream().anyMatch(t -> topic.equals(t.getName()));
                if (!topicExists) {
                    response.setStatus(HttpStatus.NOT_FOUND.value());
                    jsonMapper.writer().writeValue(outputStream, new Problem("topic not found"));
                }

                // check if partition exists (todo)
                // check if offset is correct (todo)

                final EventStreamConfig streamConfig = EventStreamConfig.builder()
                        .withTopic(topic)
                        .withCursors(ImmutableMap.of(partition, startFrom))
                        .withBatchLimit(batchLimit)
                        .withStreamLimit(ofNullable(streamLimit))
                        .withBatchTimeout(ofNullable(batchTimeout))
                        .withStreamTimeout(ofNullable(streamTimeout))
                        .withBatchKeepAliveLimit(ofNullable(batchKeepAliveLimit))
                        .build();

                response.setStatus(HttpStatus.OK.value());

                final String acceptEncoding = request.getHeader("Accept-Encoding");
                final boolean gzipEnabled = acceptEncoding != null && acceptEncoding.contains("gzip");
                final OutputStream output = gzipEnabled ? new FlushableGZIPOutputStream(outputStream) : outputStream;

                if (gzipEnabled) {
                    response.addHeader("Content-Encoding", "gzip");
                }

                final EventConsumer eventConsumer = topicRepository.createEventConsumer(topic, streamConfig.getCursors());
                final EventStream eventStream = new EventStream(eventConsumer, output, streamConfig);
                eventStream.streamEvents();

                if (gzipEnabled) {
                    output.close();
                }

            } catch (NakadiException e) {
                response.setStatus(HttpStatus.SERVICE_UNAVAILABLE.value());
                jsonMapper.writer().writeValue(outputStream, e.asProblem());
            }
            finally {
                outputStream.flush();
                outputStream.close();
            }
        };
	}

}
