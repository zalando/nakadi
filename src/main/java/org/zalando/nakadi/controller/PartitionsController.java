package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.repository.TopicRepository;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.TopicPartition;
import org.zalando.problem.Problem;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.INTERNAL_SERVER_ERROR;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private final EventTypeRepository eventTypeRepository;
    private final TimelineService timelineService;

    @Autowired
    public PartitionsController(final EventTypeRepository eventTypeRepository, final TimelineService timelineService) {
        this.eventTypeRepository = eventTypeRepository;
        this.timelineService = timelineService;
    }

    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("name") final String eventTypeName,
                                            final NativeWebRequest request) {
        LOG.trace("Get partitions endpoint for event-type '{}' is called", eventTypeName);
        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            final TopicRepository topicRepository = timelineService.getTopicRepository(eventType);
            if (!topicRepository.topicExists(eventType.getTopic())) {
                return create(Problem.valueOf(INTERNAL_SERVER_ERROR, "topic is absent in kafka"), request);
            } else {
                final List<TopicPartition> result = topicRepository
                        .loadTopicStatistics(Collections.singletonList(eventType.getTopic())).stream()
                        .map(stat -> new TopicPartition(
                                eventType.getName(),
                                stat.getPartition(),
                                // FIXME TIMELINE: IT HAS TO BE FIXED TO SUPPORT MULTIPLE TL
                                // Cursors here might be in different timeline
                                Cursor.fromTopicPosition(stat.getFirst()).getOffset(),
                                Cursor.fromTopicPosition(stat.getLast()).getOffset()))
                        .collect(Collectors.toList());
                return ok().body(result);
            }
        } catch (final NoSuchEventTypeException e) {
            return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
        } catch (final NakadiException e) {
            LOG.error("Could not list partitions. Respond with SERVICE_UNAVAILABLE.", e);
            return create(e.asProblem(), request);
        }
    }

    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public ResponseEntity<?> getPartition(@PathVariable("name") final String eventTypeName,
                                          @PathVariable("partition") final String partition,
                                          final NativeWebRequest request) {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called", eventTypeName, partition);
        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            final String topic = eventType.getTopic();

            final TopicRepository topicRepository = timelineService.getTopicRepository(eventType);
            if (!topicRepository.topicExists(topic)) {
                return create(Problem.valueOf(INTERNAL_SERVER_ERROR, "topic is absent in kafka"), request);
            } else {
                final Optional<TopicPartition> result = topicRepository.loadPartitionStatistics(topic, partition)
                        .map(tp -> new TopicPartition(
                                eventType.getName(),
                                tp.getPartition(),
                                Cursor.fromTopicPosition(tp.getFirst()).getOffset(),
                                Cursor.fromTopicPosition(tp.getLast()).getOffset()));

                if (!result.isPresent()) {
                    return create(Problem.valueOf(NOT_FOUND, "partition not found"), request);
                }
                return ok().body(result.get());
            }
        } catch (final NoSuchEventTypeException e) {
            return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
        } catch (final NakadiException e) {
            LOG.error("Could not get partition. Respond with SERVICE_UNAVAILABLE.", e);
            return create(e.asProblem(), request);
        }
    }

}
