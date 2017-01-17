package org.zalando.nakadi.stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

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

    // FIXME adyachkov: replace with reactive spring
    @RequestMapping(value = "/output-stream")
    public StreamingResponseBody stream(@Valid @RequestBody final StreamFilter streamFilter,
                                        final HttpServletResponse response) {
        return outputStream -> {

            response.setStatus(HttpStatus.OK.value());
            response.setContentType("application/x-json-stream");

            nakadiStreamService.stream(StreamConfig.newStreamConfig()
                    .setExpressions(streamFilter.getExpressions())
                    .setEventTypes(streamFilter.getEventTypes())
                    .setOutputStream(outputStream));
        };
    }

    @RequestMapping(value = "/{output_event_type_name}")
    public StreamingResponseBody toEventType(
                                 @Valid @RequestBody final StreamFilter streamFilter,
                                 @PathVariable("output_event_type_name") final String outputEventType,
                                 final HttpServletResponse response) {

        return outputStream -> {

            response.setStatus(HttpStatus.OK.value());
            response.setContentType("application/x-json-stream");

            nakadiStreamService.stream(StreamConfig.newStreamConfig()
                    .setExpressions(streamFilter.getExpressions())
                    .setEventTypes(streamFilter.getEventTypes())
                    .setOutputStream(outputStream)
                    .setOutputEventType(outputEventType));
        };
    }

}