package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.service.publishing.NakadiKpiPublisher;

import java.io.OutputStream;

@Component
public class EventStreamFactory {

    private final CursorConverter cursorConverter;
    private final EventStreamWriter eventStreamWriter;
    private final EventStreamChecks eventStreamChecks;
    private final NakadiKpiPublisher nakadiKpiPublisher;
    private final String kpiDataStreamedEventType;
    private final long kpiFrequencyMs;

    @Autowired
    public EventStreamFactory(
            final CursorConverter cursorConverter,
            final EventStreamWriter eventStreamWriter,
            final EventStreamChecks eventStreamChecks,
            final NakadiKpiPublisher nakadiKpiPublisher,
            @Value("${nakadi.kpi.event-types.nakadiDataStreamed}") final String kpiDataStreamedEventType,
            @Value("${nakadi.kpi.config.stream-data-collection-frequency-ms}") final long kpiFrequencyMs) {
        this.cursorConverter = cursorConverter;
        this.eventStreamWriter = eventStreamWriter;
        this.eventStreamChecks = eventStreamChecks;
        this.nakadiKpiPublisher = nakadiKpiPublisher;
        this.kpiDataStreamedEventType = kpiDataStreamedEventType;
        this.kpiFrequencyMs = kpiFrequencyMs;
    }

    public EventStream createEventStream(final OutputStream outputStream, final EventConsumer eventConsumer,
                                         final EventStreamConfig config, final Meter bytesFlushedMeter)
            throws InvalidCursorException {
        return new EventStream(
                eventConsumer,
                outputStream,
                config,
                eventStreamChecks,
                cursorConverter,
                bytesFlushedMeter,
                eventStreamWriter,
                nakadiKpiPublisher,
                kpiDataStreamedEventType,
                kpiFrequencyMs);
    }
}
