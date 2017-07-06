package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import java.io.OutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventConsumer;

@Component
public class EventStreamFactory {

    private final CursorConverter cursorConverter;
    private final EventStreamWriterProvider writerProvider;
    private final BlacklistService blacklistService;
    private final AuthorizationValidator authorizationValidator;

    @Autowired
    public EventStreamFactory(
            final CursorConverter cursorConverter,
            final EventStreamWriterProvider writerProvider,
            final BlacklistService blacklistService,
            final AuthorizationValidator authorizationValidator) {
        this.cursorConverter = cursorConverter;
        this.writerProvider = writerProvider;
        this.blacklistService = blacklistService;
        this.authorizationValidator = authorizationValidator;
    }


    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final Meter bytesFlushedMeter,
                                         final EventType eventType)
            throws NakadiException, InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                blacklistService,
                cursorConverter,
                bytesFlushedMeter,
                writerProvider, authorizationValidator, eventType);
    }
}
