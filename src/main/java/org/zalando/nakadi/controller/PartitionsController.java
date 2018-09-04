package org.zalando.nakadi.controller;

import com.google.common.collect.Lists;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.NakadiCursorLag;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.CursorLag;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.annotation.Nullable;
import javax.ws.rs.core.Response;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static javax.ws.rs.core.Response.Status.NOT_FOUND;
import static javax.ws.rs.core.Response.Status.SERVICE_UNAVAILABLE;
import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.spring.web.advice.Responses.create;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private final TimelineService timelineService;
    private final CursorConverter cursorConverter;
    private final CursorOperationsService cursorOperationsService;
    private static final String INVALID_CURSOR_MESSAGE = "invalid consumed_offset or partition";
    private final EventTypeRepository eventTypeRepository;
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public PartitionsController(final TimelineService timelineService,
                                final CursorConverter cursorConverter,
                                final CursorOperationsService cursorOperationsService,
                                final EventTypeRepository eventTypeRepository,
                                final AuthorizationValidator authorizationValidator) {
        this.timelineService = timelineService;
        this.cursorConverter = cursorConverter;
        this.cursorOperationsService = cursorOperationsService;
        this.eventTypeRepository = eventTypeRepository;
        this.authorizationValidator = authorizationValidator;
    }

    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public ResponseEntity<?> listPartitions(@PathVariable("name") final String eventTypeName,
                                            final NativeWebRequest request) {
        LOG.trace("Get partitions endpoint for event-type '{}' is called", eventTypeName);
        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            authorizationValidator.authorizeStreamRead(eventType);

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
                        cursorConverter.convert(selectLast(timelines, last, first)).getOffset());
            }).collect(Collectors.toList());
            return ok().body(result);
        } catch (final NoSuchEventTypeException e) {
            return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
        } catch (final InternalNakadiException e) {
            LOG.error("Could not list partitions. Respond with SERVICE_UNAVAILABLE.", e);
            return create(Problem.valueOf(SERVICE_UNAVAILABLE, e.getMessage()), request);
        }
    }

    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public ResponseEntity<?> getPartition(
            @PathVariable("name") final String eventTypeName,
            @PathVariable("partition") final String partition,
            @Nullable @RequestParam(value = "consumed_offset", required = false) final String consumedOffset,
            final NativeWebRequest request) {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called", eventTypeName, partition);
        try {
            final EventType eventType = eventTypeRepository.findByName(eventTypeName);
            authorizationValidator.authorizeStreamRead(eventType);

            if (consumedOffset != null) {
                final CursorLag cursorLag = getCursorLag(eventTypeName, partition, consumedOffset);
                return ok().body(cursorLag);
            } else {
                final EventTypePartitionView result = getTopicPartition(eventTypeName, partition);

                return ok().body(result);
            }
        } catch (final NoSuchEventTypeException e) {
            return create(Problem.valueOf(NOT_FOUND, "topic not found"), request);
        } catch (final InternalNakadiException e) {
            LOG.error("Could not get partition. Respond with SERVICE_UNAVAILABLE.", e);
            return create(Problem.valueOf(SERVICE_UNAVAILABLE, e.getMessage()), request);
        } catch (final InvalidCursorException e) {
            return create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE),
                    request);
        }
    }

    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<?> invalidCursorOperation(final InvalidCursorOperation e,
                                                    final NativeWebRequest request) {
        LOG.debug("User provided invalid cursor for operation. Reason: " + e.getReason(), e);
        return Responses.create(Problem.valueOf(MoreStatus.UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE), request);
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Problem> notFound(final NotFoundException ex, final NativeWebRequest request) {
        LOG.error(ex.getMessage(), ex);
        return Responses.create(Response.Status.NOT_FOUND, ex.getMessage(), request);
    }

    private CursorLag getCursorLag(final String eventTypeName, final String partition, final String consumedOffset)
            throws InternalNakadiException, NoSuchEventTypeException, InvalidCursorException,
            ServiceTemporarilyUnavailableException {
        final Cursor consumedCursor = new Cursor(partition, consumedOffset);
        final NakadiCursor consumedNakadiCursor = cursorConverter.convert(eventTypeName, consumedCursor);
        return cursorOperationsService.cursorsLag(eventTypeName, Lists.newArrayList(consumedNakadiCursor))
                .stream()
                .findFirst()
                .map(this::toCursorLag)
                .orElseThrow(NakadiBaseException::new);
    }

    private EventTypePartitionView getTopicPartition(final String eventTypeName, final String partition)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException {
        final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
        final Optional<PartitionStatistics> firstStats = timelineService.getTopicRepository(timelines.get(0))
                .loadPartitionStatistics(timelines.get(0), partition);
        if (!firstStats.isPresent()) {
            throw new NotFoundException("partition not found");
        }
        final NakadiCursor newest;
        if (timelines.size() == 1) {
            newest = firstStats.get().getLast();
        } else {
            final PartitionStatistics lastStats = timelineService
                    .getTopicRepository(timelines.get(timelines.size() - 1))
                    .loadPartitionStatistics(timelines.get(timelines.size() - 1), partition)
                    .get();
            newest = selectLast(timelines, lastStats, firstStats.get());
        }

        return new EventTypePartitionView(
                eventTypeName,
                partition,
                cursorConverter.convert(firstStats.get().getFirst()).getOffset(),
                cursorConverter.convert(newest).getOffset());
    }

    private CursorLag toCursorLag(final NakadiCursorLag nakadiCursorLag) {
        return new CursorLag(
                nakadiCursorLag.getPartition(),
                cursorConverter.convert(nakadiCursorLag.getFirstCursor()).getOffset(),
                cursorConverter.convert(nakadiCursorLag.getLastCursor()).getOffset(),
                nakadiCursorLag.getLag()
        );
    }


    private static NakadiCursor selectLast(final List<Timeline> activeTimelines, final PartitionEndStatistics last,
                                           final PartitionStatistics first) {
        final NakadiCursor lastInLastTimeline = last.getLast();
        if (!lastInLastTimeline.isInitial()) {
            return lastInLastTimeline;
        }
        // There may be a situation, when there is no data in all the timelines after first, but any cursor from the
        // next after first timelines is greater then the cursor in first timeline. Therefore we need to roll pointer
        // to the end back till the very beginning or to the end of first timeline with data.
        for (int idx = activeTimelines.size() - 2; idx > 0; --idx) {
            final Timeline timeline = activeTimelines.get(idx);
            final NakadiCursor lastInTimeline = timeline.getLatestPosition()
                    .toNakadiCursor(timeline, first.getPartition());
            if (!lastInTimeline.isInitial()) {
                return lastInTimeline;
            }
        }
        return first.getLast();
    }

}
