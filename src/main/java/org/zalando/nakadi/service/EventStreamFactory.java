package org.zalando.nakadi.service;

import java.io.OutputStream;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventConsumer;

@Component
public class EventStreamFactory {

    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final BlacklistService blacklistService)
            throws NakadiException, InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                blacklistService);
    }
}
