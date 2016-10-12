package org.zalando.nakadi.service;

import org.zalando.nakadi.repository.EventConsumer;
import org.springframework.stereotype.Component;

import java.io.OutputStream;

@Component
public class EventStreamFactory {

    public EventStream createEventStream(final EventConsumer eventConsumer,
                                         final OutputStream outputStream,
                                         final EventStreamConfig config,
                                         final FloodService floodService) {
        return new EventStream(eventConsumer, outputStream, config, floodService);
    }
}
