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
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.security.Client;
import org.zalando.nakadi.service.timeline.TimelineService;
import org.zalando.nakadi.view.TimelineRequest;

import javax.ws.rs.core.MediaType;

@RestController
@RequestMapping(value = "/timelines", produces = MediaType.APPLICATION_JSON)
public class TimelinesController {

    private static final Logger LOG = LoggerFactory.getLogger(TimelinesController.class);

    private final SecuritySettings securitySettings;
    private final TimelineService timelineService;

    @Autowired
    public TimelinesController(final SecuritySettings securitySettings,
                               final TimelineService timelineService) {
        this.securitySettings = securitySettings;
        this.timelineService = timelineService;
    }

    @RequestMapping(method = RequestMethod.POST)
    public ResponseEntity<?> createStorage(@RequestBody final TimelineRequest timelineRequest, final Client client) {
        timelineService.createTimeline(timelineRequest, client);
        return ResponseEntity.status(HttpStatus.CREATED).build();
    }

    @ResponseStatus(value = HttpStatus.FORBIDDEN)
    @ExceptionHandler(ForbiddenAccessException.class)
    public void forbidden(final Exception ex) {
        LOG.debug("Request is forbidden", ex);
    }

}
