package de.zalando.aruha.nakadi.service;

import de.zalando.aruha.nakadi.repository.EventConsumer;

import java.io.OutputStream;

public class EventStreamFactory {

    public EventStream createEventStream(final EventConsumer eventConsumer, final OutputStream outputStream,
                                         final EventStreamConfig config) {
        return new EventStream(eventConsumer, outputStream, config);
    }
}
