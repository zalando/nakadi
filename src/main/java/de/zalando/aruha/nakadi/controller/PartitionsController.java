package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.exceptions.NakadiException;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.problem.Problem;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private final TopicRepository topicRepository;

    public PartitionsController(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    @Timed(name = "get_partitions", absolute = true)
    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("name") final String eventTypeName,
                                            final NativeWebRequest request) {
        LOG.trace("Get partitions endpoint for event-type '{}' is called", eventTypeName);
        try {
            @SuppressWarnings("UnnecessaryLocalVariable")
            final String topic = eventTypeName;

            if (topicRepository.topicExists(topic)) {
                return ok().body(topicRepository.listPartitions(topic));
            }
            else {
                return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
            }
        }
        catch (final NakadiException e) {
            LOG.error("Could not list partitions. Respond with SERVICE_UNAVAILABLE.", e);
            return create(e.asProblem(), request);
        }
    }

    @Timed(name = "get_partition", absolute = true)
    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public ResponseEntity<?> getPartition(@PathVariable("name") final String eventTypeName,
                                          @PathVariable("partition") final String partition,
                                          final NativeWebRequest request) {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called", eventTypeName, partition);
        try {
            @SuppressWarnings("UnnecessaryLocalVariable")
            final String topic = eventTypeName;

            if (!topicRepository.topicExists(topic)) {
                return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
            }
            else if (!topicRepository.partitionExists(topic, partition)) {
                return create(Problem.valueOf(NOT_FOUND, "partition not found"), request);
            }
            else {
                return ok().body(topicRepository.getPartition(topic, partition));
            }
        }
        catch (final NakadiException e) {
            LOG.error("Could not get partition. Respond with SERVICE_UNAVAILABLE.", e);
            return create(e.asProblem(), request);
        }
    }

}
