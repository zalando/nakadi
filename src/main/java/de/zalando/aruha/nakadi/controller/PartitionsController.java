package de.zalando.aruha.nakadi.controller;

import com.codahale.metrics.annotation.Timed;
import de.zalando.aruha.nakadi.NakadiException;
import de.zalando.aruha.nakadi.domain.Problem;
import de.zalando.aruha.nakadi.repository.TopicRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import static org.springframework.http.ResponseEntity.ok;
import static org.springframework.http.ResponseEntity.status;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private TopicRepository topicRepository;

    public PartitionsController(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

    @Timed(name = "get_partitions", absolute = true)
    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("name") final String eventTypeName,
                                            @RequestHeader(name = "X-Flow-Id", required = false) final String flowId) {
        LOG.trace("Get partitions endpoint for event-type '{}' is called for flow id: {}", eventTypeName, flowId);
        try {
            // todo: we should get topic from EventType after persistence of EventType is implemented
            @SuppressWarnings("UnnecessaryLocalVariable")
            final String topic = eventTypeName;

            if (topicRepository.topicExists(topic)) {
                return ok().body(topicRepository.listPartitions(topic));
            }
            else {
                return status(HttpStatus.NOT_FOUND).body(new Problem("topic not found"));
            }
        }
        catch (final NakadiException e) {
            return status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getProblemMessage());
        }
        catch (final Exception e) {
            return status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

    @Timed(name = "get_partition", absolute = true)
    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public ResponseEntity<?> getPartition(@PathVariable("name") final String eventTypeName,
                                          @PathVariable("partition") final String partition,
                                          @RequestHeader(name = "X-Flow-Id", required = false) String flowId) {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called for flow id: {}", eventTypeName,
                partition, flowId);
        try {
            // todo: we should get topic from EventType after persistence of EventType is implemented
            @SuppressWarnings("UnnecessaryLocalVariable")
            final String topic = eventTypeName;

            if (!topicRepository.topicExists(topic)) {
                return status(HttpStatus.NOT_FOUND).body(new Problem("topic not found"));
            }
            else if (!topicRepository.partitionExists(topic, partition)) {
                return status(HttpStatus.NOT_FOUND).body(new Problem("partition not found"));
            }
            else {
                return ok().body(topicRepository.getPartition(topic, partition));
            }
        }
        catch (final NakadiException e) {
            return status(HttpStatus.SERVICE_UNAVAILABLE).body(e.getProblemMessage());
        }
        catch (final Exception e) {
            return status(HttpStatus.INTERNAL_SERVER_ERROR).body(e.getMessage());
        }
    }

}
