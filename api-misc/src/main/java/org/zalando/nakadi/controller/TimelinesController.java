package org.zalando.nakadi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.InconsistentStateException;
import org.zalando.nakadi.exceptions.runtime.RepositoryProblemException;
import org.zalando.nakadi.exceptions.runtime.TimelineException;
import org.zalando.nakadi.exceptions.runtime.TopicRepositoryException;
import org.zalando.nakadi.service.publishing.NakadiAuditLogPublisher;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.TimelineRequest;
import org.zalando.nakadi.view.TimelineView;

import java.util.Optional;
import java.util.stream.Collectors;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RestController
@RequestMapping(value = "/event-types/{name}/timelines", produces = APPLICATION_JSON_VALUE)
public class TimelinesController {

    private final TimelineService timelineService;
    private final NakadiAuditLogPublisher auditLogPublisher;

    @Autowired
    public TimelinesController(final TimelineService timelineService,
                               final NakadiAuditLogPublisher auditLogPublisher) {
        this.timelineService = timelineService;
        this.auditLogPublisher = auditLogPublisher;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createTimeline(@PathVariable("name") final String eventTypeName,
                                            @RequestBody final TimelineRequest timelineRequest,
                                            final NativeWebRequest request)
            throws AccessDeniedException, TimelineException, TopicRepositoryException, InconsistentStateException,
            RepositoryProblemException {
        final var nextTimeline = timelineService.createTimeline(eventTypeName, timelineRequest.getStorageId());

        auditLogPublisher.publish(
                Optional.empty(),
                Optional.of(nextTimeline),
                NakadiAuditLogPublisher.ResourceType.TIMELINE,
                NakadiAuditLogPublisher.ActionType.CREATED,
                String.valueOf(nextTimeline.getId()));

        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @RequestMapping(method = RequestMethod.GET)
    public ResponseEntity<?> getTimelines(@PathVariable("name") final String eventTypeName) {
        return ResponseEntity.ok(timelineService.getTimelines(eventTypeName).stream()
                .map(TimelineView::new)
                .collect(Collectors.toList()));
    }
}
