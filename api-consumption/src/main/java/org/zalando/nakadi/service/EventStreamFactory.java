package org.zalando.nakadi.service;

import com.codahale.metrics.Meter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.runtime.InvalidCursorException;
import org.zalando.nakadi.repository.EventConsumer;

import java.io.OutputStream;

@Component
public class EventStreamFactory {

    private final CursorConverter cursorConverter;
    private final EventStreamJsonWriter eventStreamWriter;
    private final EventStreamChecks eventStreamChecks;
    private final ConsumptionKpiCollectorFactory kpiCollectorFactory;

    @Autowired
    public EventStreamFactory(
            final CursorConverter cursorConverter,
            final EventStreamJsonWriter eventStreamWriter,
            final EventStreamChecks eventStreamChecks,
            final ConsumptionKpiCollectorFactory kpiCollectorFactory) {
        this.cursorConverter = cursorConverter;
        this.eventStreamWriter = eventStreamWriter;
        this.eventStreamChecks = eventStreamChecks;
        this.kpiCollectorFactory = kpiCollectorFactory;
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
                kpiCollectorFactory.createForLoLA(config.getConsumingClient()));
    }
}
