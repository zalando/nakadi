package org.zalando.nakadi.service;

import java.io.OutputStream;

import com.codahale.metrics.Meter;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.util.FeatureToggleService;

@Component
public class EventStreamFactory {

    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final BlacklistService blacklistService,
                                         final CursorConverter cursorConverter, final Meter bytesFlushedMeter,
                                         final FeatureToggleService featureToggleService)
            throws NakadiException, InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                blacklistService,
                cursorConverter,
                bytesFlushedMeter,
                featureToggleService);
    }
}
