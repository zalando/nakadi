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
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorOperation;
import org.zalando.nakadi.exceptions.runtime.MyNakadiRuntimeException1;
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
import org.zalando.problem.Problem;

import javax.annotation.Nullable;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.springframework.http.ResponseEntity.ok;
import static org.zalando.problem.Status.FORBIDDEN;
import static org.zalando.problem.Status.NOT_FOUND;
import static org.zalando.problem.Status.SERVICE_UNAVAILABLE;
import static org.zalando.problem.Status.UNPROCESSABLE_ENTITY;

@RestController
public class PartitionsController implements NakadiProblemHandling {

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
                                            final NativeWebRequest request) throws Throwable {
        LOG.trace("Get partitions endpoint for event-type '{}' is called", eventTypeName);
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
                    cursorConverter.convert(last.getLast()).getOffset());
        }).collect(Collectors.toList());
        return ok().body(result);
    }

    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public ResponseEntity<?> getPartition(@PathVariable("name") final String eventTypeName,
                                          @PathVariable("partition") final String partition,
                                          @Nullable @RequestParam(value = "consumed_offset", required = false)
                                          final String consumedOffset, final NativeWebRequest request)
            throws InvalidCursorException {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called", eventTypeName, partition);
        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        authorizationValidator.authorizeStreamRead(eventType);

        if (consumedOffset != null) {
            final CursorLag cursorLag = getCursorLag(eventTypeName, partition, consumedOffset);
            return ok().body(cursorLag);
        } else {
            final EventTypePartitionView result = getTopicPartition(eventTypeName, partition);

            return ok().body(result);
        }
    }

    @ExceptionHandler(AccessDeniedException.class)
    public ResponseEntity<Problem> handleAccessDeniedException(final AccessDeniedException exception,
                                                               final NativeWebRequest request) {
        return create(Problem.valueOf(FORBIDDEN, exception.explain()), request);
    }

    @ExceptionHandler(InternalNakadiException.class)
    public ResponseEntity<?> handleInternalNakadiException(final InternalNakadiException exception,
                                                           final NativeWebRequest request) {
        LOG.debug("Could not get partition. Respond with SERVICE_UNAVAILABLE.", exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    @ExceptionHandler(InvalidCursorException.class)
    public ResponseEntity<?> handleInvalidCursorException(final InvalidCursorException exception,
                                                          final NativeWebRequest request) {
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE), request);
    }

    @ExceptionHandler(InvalidCursorOperation.class)
    public ResponseEntity<?> handleInvalidCursorOperation(final InvalidCursorOperation exception,
                                                          final NativeWebRequest request) {
        LOG.debug("User provided invalid cursor for operation. Reason: " + exception.getReason(), exception);
        return create(Problem.valueOf(UNPROCESSABLE_ENTITY, INVALID_CURSOR_MESSAGE), request);
    }

    @Override
    @ExceptionHandler(NoSuchEventTypeException.class)
    public ResponseEntity<Problem> handleNoSuchEventTypeException(final NoSuchEventTypeException exception,
                                                                  final NativeWebRequest request) {
        LOG.debug(exception.getMessage());
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<Problem> handleNotFoundException(final NotFoundException exception,
                                                           final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(NOT_FOUND, exception.getMessage()), request);
    }

    @ExceptionHandler(ServiceTemporarilyUnavailableException.class)
    public ResponseEntity<Problem> handleServiceTemporarilyUnavailableException(
            final ServiceTemporarilyUnavailableException exception, final NativeWebRequest request) {
        LOG.error(exception.getMessage(), exception);
        return create(Problem.valueOf(SERVICE_UNAVAILABLE, exception.getMessage()), request);
    }

    private EventTypePartitionView getTopicPartition(final String eventTypeName, final String partition)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException {
        final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
        final Optional<PartitionStatistics> firstStats = timelineService.getTopicRepository(timelines.get(0))
                .loadPartitionStatistics(timelines.get(0), partition);
        if (!firstStats.isPresent()) {
            throw new NotFoundException("partition not found");
        }
        final PartitionStatistics lastStats;
        if (timelines.size() == 1) {
            lastStats = firstStats.get();
        } else  {
            lastStats = timelineService.getTopicRepository(timelines.get(timelines.size() - 1))
                    .loadPartitionStatistics(timelines.get(timelines.size() - 1), partition).get();
        }

        return new EventTypePartitionView(
                eventTypeName,
                lastStats.getPartition(),
                cursorConverter.convert(firstStats.get().getFirst()).getOffset(),
                cursorConverter.convert(lastStats.getLast()).getOffset());
    }

    private CursorLag toCursorLag(final NakadiCursorLag nakadiCursorLag) {
        return new CursorLag(
                nakadiCursorLag.getPartition(),
                cursorConverter.convert(nakadiCursorLag.getFirstCursor()).getOffset(),
                cursorConverter.convert(nakadiCursorLag.getLastCursor()).getOffset(),
                nakadiCursorLag.getLag()
        );
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
                .orElseThrow(MyNakadiRuntimeException1::new);
    }
}
