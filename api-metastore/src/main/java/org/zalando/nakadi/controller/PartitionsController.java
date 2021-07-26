package org.zalando.nakadi.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.CursorError;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.domain.PartitionEndStatistics;
import org.zalando.nakadi.domain.PartitionStatistics;
import org.zalando.nakadi.domain.Timeline;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.NakadiBaseException;
import org.zalando.nakadi.exceptions.runtime.NoSuchEventTypeException;
import org.zalando.nakadi.exceptions.runtime.NotFoundException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnparseableCursorException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.EventTypeRepository;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.service.AuthorizationValidator;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.RepartitioningService;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.Cursor;
import org.zalando.nakadi.view.EventTypePartitionView;
import org.zalando.nakadi.view.PartitionCountView;
import static org.springframework.http.ResponseEntity.noContent;


import javax.annotation.Nullable;
import static java.util.Collections.singletonList;
import static java.util.Collections.emptyList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

@RestController
public class PartitionsController {

    private static final Logger LOG = LoggerFactory.getLogger(PartitionsController.class);

    private final TimelineService timelineService;
    private final CursorConverter cursorConverter;
    private final CursorOperationsService cursorOperationsService;
    private final AuthorizationValidator authorizationValidator;
    private final EventTypeCache eventTypeCache;
    private final EventTypeRepository eventTypeRepository;
    private final AdminService adminService;
    private final RepartitioningService repartitioningService;
    private final ObjectMapper objectMapper;

    @Autowired
    public PartitionsController(final TimelineService timelineService,
                                final CursorConverter cursorConverter,
                                final CursorOperationsService cursorOperationsService,
                                final EventTypeCache eventTypeCache,
                                final EventTypeRepository eventTypeRepository,
                                final AuthorizationValidator authorizationValidator,
                                final AdminService adminService,
                                final RepartitioningService repartitioningService,
                                final ObjectMapper objectMapper
                                ) {
        this.timelineService = timelineService;
        this.cursorConverter = cursorConverter;
        this.cursorOperationsService = cursorOperationsService;
        this.authorizationValidator = authorizationValidator;
        this.eventTypeCache = eventTypeCache;
        this.eventTypeRepository = eventTypeRepository;
        this.adminService = adminService;
        this.repartitioningService = repartitioningService;
        this.objectMapper = objectMapper;
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

    @RequestMapping(value = "/event-types/{name}/partitions", method = RequestMethod.GET)
    public List<EventTypePartitionView> listPartitions(
            @PathVariable("name") final String eventTypeName,
            @RequestParam(value = "cursors", required = false) final String cursorsString
    ) throws NoSuchEventTypeException {
        LOG.trace(
                "Get partitions endpoint for event-type '{}' with cursorList `{}` is called",
                eventTypeName, cursorsString);
        checkAuthorization(eventTypeName);

        final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
        final List<PartitionStatistics> firstStats =
                timelineService.getTopicRepository(timelines.get(0))
                .loadTopicStatistics(singletonList(timelines.get(0)));
        final List<PartitionStatistics> lastStats = initializeLastStats(timelines, firstStats);

        final List<Cursor> cursorList = getParsedCursors(cursorsString, eventTypeName);
        final List<PartitionStatistics> filteredFirstStats;
        final List<EventTypePartitionView> cursorLags;

        if(!cursorList.isEmpty()){
            final Map<String, List<Cursor>> cursorsGroupByPartition = cursorList
                    .stream()
                    .collect(Collectors.groupingBy(Cursor::getPartition));

            checkDuplicatePartitionIds(cursorsGroupByPartition);

            //exclude whats present in cursorList
            filteredFirstStats = firstStats.stream()
                    .filter(pStat -> !cursorsGroupByPartition.containsKey(pStat.getPartition()))
                    .collect(Collectors.toList());

            cursorLags = getViewsWithUnconsumedEvents(eventTypeName, cursorList);
        }else {
            //if no cursor supplied, default to original behaviour
            filteredFirstStats = firstStats;
            cursorLags = emptyList();
        }

        final List<EventTypePartitionView> result =
                getViewsByMatchingSamePartitionAcrossStats(
                        timelines,
                        filteredFirstStats,
                        lastStats);
        result.addAll(cursorLags);
        return result;
    }

    private List<EventTypePartitionView> getViewsByMatchingSamePartitionAcrossStats(
            final List<Timeline> timelines,
            final List<PartitionStatistics> filteredFirstStats,
            final List<PartitionStatistics> lastStats) {
        //For each partition in first stat, find the matching one in last stat
        //and create EventTypePartitionView
        //special handling for finding correct last  partition is done in selectLast()
        return filteredFirstStats.stream().map(first -> {
            final PartitionStatistics last = lastStats.stream()
                    .filter(l -> l.getPartition().equals(first.getPartition()))
                    .findAny().get();
            return new EventTypePartitionView(
                    first.getPartition(),
                    cursorConverter.convert(first.getFirst()).getOffset(),
                    cursorConverter.convert(selectLast(timelines, last, first)).getOffset());
        }).collect(Collectors.toList());
    }

    private List<Cursor> getParsedCursors(final String cursorsString, final String eventType){
        if(StringUtils.isEmpty(cursorsString)){
            return emptyList();
        }
        try {
            final List<Cursor> cursorList = objectMapper.
                    readValue(
                        cursorsString,
                        new TypeReference<List<Cursor>>() {
                    });

            validateCursors(cursorList, eventType);
            return cursorList;
        } catch (JsonProcessingException e) {
            throw new UnparseableCursorException("malformed cursors", e, cursorsString);
        }

    }

    private void validateCursors(final List<Cursor> cursorList, final String eventType) {
        for (final Cursor cursor : cursorList) {
            if (StringUtils.isEmpty(cursor.getPartition())) {
                throw new InvalidCursorException(CursorError.NULL_PARTITION, cursor, eventType);
            } else if (StringUtils.isEmpty(cursor.getOffset())) {
                throw new InvalidCursorException(CursorError.NULL_OFFSET, cursor, eventType);
            }
        }
    }

    private List<PartitionStatistics> initializeLastStats( final List<Timeline> timelines,
                                                           final List<PartitionStatistics> firstStats) {
        final List<PartitionStatistics> lastStats;
        if (timelines.size() == 1) {
            lastStats = firstStats;
        } else {
            lastStats = timelineService.
                    getTopicRepository(timelines.get(timelines.size() - 1))
                    .loadTopicStatistics(singletonList(timelines.get(timelines.size() - 1)));
        }
        return lastStats;
    }

    private List<EventTypePartitionView> getViewsWithUnconsumedEvents
            (final String eventTypeName,
             final List<Cursor> cursorList) {

        final List<EventTypePartitionView> cursorLags
                = cursorOperationsService
                .toCursorLagStream(
                        cursorList,
                        eventTypeName,
                        cursorConverter)
                .map(cl -> new EventTypePartitionView(
                                cl.getPartition(),
                                cl.getOldestAvailableOffset(),
                                cl.getNewestAvailableOffset(),
                                cl.getUnconsumedEvents())
                ).collect(Collectors.toList());

        if (cursorLags.size() != cursorList.size()){
            throw new NakadiBaseException(String.format("Problem with cursors '%s'", cursorLags));
        }
        return cursorLags;
    }

    private void checkDuplicatePartitionIds(
            final Map<String, List<Cursor>> cursorsGroupByPartition) {

        final boolean duplicatePartitionExists =
                cursorsGroupByPartition.values().stream()
                .anyMatch(list -> list.size() > 1);
        if(duplicatePartitionExists) {
            throw new UnprocessableEntityException("duplicate partition ids provided in cursors");
        }
    }

    private void checkAuthorization(final String eventTypeName) {
        final EventType eventType = eventTypeCache.getEventType(eventTypeName);
        authorizationValidator.authorizeEventTypeView(eventType);
        authorizationValidator.authorizeStreamRead(eventType);
    }

    @RequestMapping(value = "/event-types/{name}/partitions/{partition}", method = RequestMethod.GET)
    public EventTypePartitionView getPartition(
            @PathVariable("name") final String eventTypeName,
            @PathVariable("partition") final String partition,
            @Nullable @RequestParam(value = "consumed_offset", required = false) final String consumedOffset)
            throws NoSuchEventTypeException {
        LOG.trace("Get partition endpoint for event-type '{}', partition '{}' is called", eventTypeName, partition);
        checkAuthorization(eventTypeName);

        if (consumedOffset != null) {
            return getViewsWithUnconsumedEvents(
                        eventTypeName,
                        singletonList(new Cursor(partition, consumedOffset)))
                    .get(0);
        } else {
            return getTopicPartition(eventTypeName, partition);
        }
    }

    @PutMapping(value = "/event-types/{name}/partition-count")
    public ResponseEntity<Void> repartition(
            @PathVariable("name") final String eventTypeName,
            @RequestBody final PartitionCountView partitionsNumberView) throws NoSuchEventTypeException {
        final EventType eventType = eventTypeRepository.findByName(eventTypeName);
        if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, eventType.asResource());
        }

        repartitioningService.repartition(eventType.getName(), partitionsNumberView.getPartitionCount());
        return noContent().build();
    }

    private EventTypePartitionView getTopicPartition(final String eventTypeName, final String partition)
            throws InternalNakadiException, NoSuchEventTypeException, ServiceTemporarilyUnavailableException {
        final List<Timeline> timelines = timelineService.getActiveTimelinesOrdered(eventTypeName);
        final Optional<PartitionStatistics> firstStats = timelineService.getTopicRepository(timelines.get(0))
                .loadPartitionStatistics(timelines.get(0), partition);
        if (firstStats.isEmpty()) {
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
                partition,
                cursorConverter.convert(firstStats.get().getFirst()).getOffset(),
                cursorConverter.convert(newest).getOffset());
    }

}
