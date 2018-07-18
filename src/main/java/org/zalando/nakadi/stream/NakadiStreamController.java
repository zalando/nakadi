package org.zalando.nakadi.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import org.zalando.nakadi.security.Client;

import javax.servlet.http.HttpServletResponse;
import javax.validation.Valid;

@RestController
@RequestMapping(value = "/streams")
public final class NakadiStreamController {

    private static final Logger LOG = LoggerFactory.getLogger(NakadiStreamController.class);
    private final NakadiStreamService nakadiStreamService;

    @Autowired
    public NakadiStreamController(final NakadiStreamService nakadiStreamService) {
        this.nakadiStreamService = nakadiStreamService;
    }

    @RequestMapping(method = RequestMethod.POST, value = "/start")
    public ResponseEntity<StopRequest> start(@Valid @RequestBody final StartRequest startRequest,
                                             final HttpServletResponse response,
                                             final Client client) {
        response.setStatus(HttpStatus.OK.value());
        response.setContentType("application/x-json-stream");
        final String streamId = nakadiStreamService.start(StreamConfig.newStreamConfig()
                .setApplicationId(client.getClientId())
                .setFromEventTypes(startRequest.getFromEventTypes())
                .setToEventType(startRequest.getToEventType()));
        return ResponseEntity.ok().body(new StopRequest(streamId));
    }

    @RequestMapping(method = RequestMethod.POST, value = "/stop")
    public ResponseEntity<?> stop(@Valid @RequestBody final StopRequest stopRequest,
                                  final HttpServletResponse response) {
        response.setStatus(HttpStatus.OK.value());
        response.setContentType("application/x-json-stream");
        nakadiStreamService.stop(stopRequest.getStreamId());
        return ResponseEntity.ok().build();
    }

}
