package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventConsumer;

import java.io.OutputStream;

@Component
public class EventStreamFactory {

    private final CursorConverter cursorConverter;
    private final EventStreamWriterProvider writerProvider;
    private final BlacklistService blacklistService;

    @Autowired
    public EventStreamFactory(
            final CursorConverter cursorConverter,
            final EventStreamWriterProvider writerProvider,
            final BlacklistService blacklistService) {
        this.cursorConverter = cursorConverter;
        this.writerProvider = writerProvider;
        this.blacklistService = blacklistService;
    }


    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final Meter bytesFlushedMeter)
            throws NakadiException, InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                blacklistService,
                cursorConverter,
                bytesFlushedMeter,
                writerProvider);
    }
}
