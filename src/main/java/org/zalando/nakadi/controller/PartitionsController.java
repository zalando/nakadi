package org.zalando.nakadi.controller;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.exceptions.NoSuchEventTypeException;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.problem.Problem;
import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private final TimelineService timelineService;
    private final CursorConverter cursorConverter;

    @Autowired
    public PartitionsController(final TimelineService timelineService,
                                final CursorConverter cursorConverter) {
        this.timelineService = timelineService;
        this.cursorConverter = cursorConverter;
    }

    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("name") final String eventTypeName,
                                            final NativeWebRequest request) {
        LOG.trace("Get partitions endpoint for event-type '{}' is called", eventTypeName);
        try {
            final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
            final List<PartitionStatistics> firstStats = timelineService.getTopicRepository(timelines.get(0))
                    .loadTopicStatistics(Collections.singletonList(timelines.get(0)));
            final List<PartitionStatistics> lastStats;
            if (timelines.size() == 1) {
                lastStats = firstStats;
            } else {
                lastStats = timelineService.getTopicRepository(timelines.get(timelines.size() - 1))
                        .loadTopicStatistics(Collections.singletonList(timelines.get(timelines.size() - 1)));
            }
            final List<EventTypePartitionView> result = firstStats.stream().map(first -> {
                final PartitionStatistics last = lastStats.stream()
                        .filter(l -> l.getPartition().equals(first.getPartition()))
                        .findAny().get();
                return new EventTypePartitionView(
                        eventTypeName,
                        first.getPartition(),
                        cursorConverter.convert(first.getFirst()).getOffset(),
                        cursorConverter.convert(last.getLast()).getOffset());
            }).collect(Collectors.toList());
            return ok().body(result);
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
            final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
            final Optional<PartitionStatistics> firstStats = timelineService.getTopicRepository(timelines.get(0))
                    .loadPartitionStatistics(timelines.get(0), partition);
            if (!firstStats.isPresent()) {
                return create(Problem.valueOf(NOT_FOUND, "partition not found"), request);
            }
            final PartitionStatistics lastStats;
            if (timelines.size() == 1) {
                lastStats = firstStats.get();
            } else  {
                lastStats = timelineService.getTopicRepository(timelines.get(timelines.size() - 1))
                        .loadPartitionStatistics(timelines.get(timelines.size() - 1), partition).get();
            }
            final EventTypePartitionView result = new EventTypePartitionView(
                    eventTypeName,
                    lastStats.getPartition(),
                    cursorConverter.convert(firstStats.get().getFirst()).getOffset(),
                    cursorConverter.convert(lastStats.getLast()).getOffset());
            return ok().body(result);
        } catch (final NoSuchEventTypeException e) {
            return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
        } catch (final NakadiException e) {
            LOG.error("Could not get partition. Respond with SERVICE_UNAVAILABLE.", e);
            return create(e.asProblem(), request);
        }
    }

}
