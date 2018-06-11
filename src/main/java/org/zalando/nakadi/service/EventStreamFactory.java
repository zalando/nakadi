package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.repository.EventConsumer;

import java.io.OutputStream;

@Component
public class EventStreamFactory {

    private final CursorConverter cursorConverter;
    private final EventStreamWriterProvider writerProvider;
    private final BlacklistService blacklistService;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String kpiDataStreamedEventType;
    private final long kpiFrequencyMs;

    @Autowired
    public EventStreamFactory(
            final CursorConverter cursorConverter,
            final EventStreamWriterProvider writerProvider,
            final BlacklistService blacklistService,
            final NakadiKpiPublisher nakadiKpiPublisher,
            @Value("${nakadi.kpi.event-types.nakadiDataStreamed}") final String kpiDataStreamedEventType,
            @Value("${nakadi.kpi.config.stream-data-collection-frequency-ms}") final long kpiFrequencyMs) {
        this.cursorConverter = cursorConverter;
        this.writerProvider = writerProvider;
        this.blacklistService = blacklistService;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.kpiDataStreamedEventType = kpiDataStreamedEventType;
        this.kpiFrequencyMs = kpiFrequencyMs;
    }

    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final Meter bytesFlushedMeter)
            throws InternalNakadiException, InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                blacklistService,
                cursorConverter,
                bytesFlushedMeter,
                writerProvider,
                nakadiKpiPublisher,
                kpiDataStreamedEventType,
                kpiFrequencyMs);
    }
}
