package org.zalando.nakadi.controller;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.context.request.NativeWebRequest;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.runtime.NoTopicFoundException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.TopicsService;
import org.zalando.problem.Problem;
import org.zalando.problem.spring.web.advice.Responses;

import javax.ws.rs.core.Response;

@RestController
@RequestMapping(value = "/topics/")
public class TopicsController {

    private static final Logger LOG = LoggerFactory.getLogger(TimelinesController.class);

    private final TopicsService topicsService;

    @Autowired
    public TopicsController(final TopicsService topicsService) {
        this.topicsService = topicsService;
    }

    @RequestMapping(value = "{topic}", method = RequestMethod.GET)
    public ResponseEntity<EventType> getEventTypeByTopic(@PathVariable("topic") final String topic, final Client client) {
        return ResponseEntity.ok(topicsService.getEventTypeByTopic(topic, client));
    }

    @ExceptionHandler(ForbiddenAccessException.class)
    public ResponseEntity<Problem> forbidden(final ForbiddenAccessException ex, final NativeWebRequest request) {
        LOG.error(ex.getMessage(), ex);
        return Responses.create(Response.Status.FORBIDDEN, ex.getMessage(), request);
    }

    @ExceptionHandler(NoTopicFoundException.class)
    public ResponseEntity<Problem> noTopicFound(final NoTopicFoundException ex, final NativeWebRequest request) {
        LOG.error(ex.getMessage(), ex);
        return Responses.create(Response.Status.NOT_FOUND, ex.getMessage(), request);
    }

}
