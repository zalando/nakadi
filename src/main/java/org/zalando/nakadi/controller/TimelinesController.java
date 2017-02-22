package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.TimelineRequest;
import org.zalando.problem.MoreStatus;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@RestController
@RequestMapping(value = "/timelines", produces = MediaType.APPLICATION_JSON)
public class TimelinesController {

    private static final Logger LOG = LoggerFactory.getLogger(TimelinesController.class);

    private final TimelineService timelineService;

    @Autowired
    public TimelinesController(final TimelineService timelineService) {
        this.timelineService = timelineService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createTimeline(@RequestBody final TimelineRequest timelineRequest, final Client client) {
        timelineService.createTimeline(timelineRequest.getEventType(), timelineRequest.getStorageId(), client);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @ExceptionHandler(ForbiddenAccessException.class)
    public ResponseEntity<Problem> forbidden(final ForbiddenAccessException ex, final NativeWebRequest request) {
        LOG.error(ex.getMessage(), ex);
        return Responses.create(Response.Status.FORBIDDEN, ex.getMessage(), request);
    }

    @ExceptionHandler(UnableProcessException.class)
    public ResponseEntity<Problem> notFound(final UnableProcessException ex, final NativeWebRequest request) {
        LOG.error(ex.getMessage(), ex);
        return Responses.create(MoreStatus.UNPROCESSABLE_ENTITY, ex.getMessage(), request);
    }

}
