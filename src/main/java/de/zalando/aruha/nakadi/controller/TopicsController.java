package de.zalando.aruha.nakadi.controller;

import java.util.List;

import static org.springframework.http.ResponseEntity.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import com.codahale.metrics.annotation.Metered;
import com.codahale.metrics.annotation.Timed;

import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.config.NakadiConfig;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.domain.Topic;
import de.zalando.aruha.nakadi.domain.TopicPartition;
import de.zalando.aruha.nakadi.repository.TopicRepository;

import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;

@RestController
@RequestMapping(value = "/topics")
public class TopicsController {
	private static final Logger LOG = LoggerFactory.getLogger(TopicsController.class);
	@Autowired
	private TopicRepository topicRepository;

	@Timed
	@RequestMapping(value = "/", method = RequestMethod.GET)
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
	@RequestMapping(value = "/{topicId}", method = RequestMethod.GET)
	public void getTopic(@PathVariable("topicId") final String topicId) {
		throw new UnsupportedOperationException();
	}

	@Timed
	@RequestMapping(value = "/{topicId}/partitions", method = RequestMethod.GET)
	public ResponseEntity<?> listPartitions(@PathVariable("topicId") final String topicId) {
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
	@RequestMapping(value = "/{topicId}/partitions/{partitionId}", method = RequestMethod.POST)
	public ResponseEntity<?> postEventToPartition(@PathVariable("topicId") final String topicId,
			@PathVariable("partitionId") final String partitionId, @RequestBody final String messagePayload) {
		LOG.trace("Event received: {}, {}, {}", new Object[] { topicId, partitionId, messagePayload });
		topicRepository.postEvent(topicId, partitionId, messagePayload);
		return ok().build();
	}

}
