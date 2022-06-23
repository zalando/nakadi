package org.zalando.nakadi.service;

import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.service.subscription.StreamContentType;

@Component
public class EventStreamWriterFactory {

    private final EventStreamJsonWriter eventStreamJsonWriter;
    private final EventStreamBinaryWriter eventStreamBinaryWriter;

    public EventStreamWriterFactory(
            final EventStreamJsonWriter eventStreamJsonWriter,
            final EventStreamBinaryWriter eventStreamBinaryWriter) {
        this.eventStreamJsonWriter = eventStreamJsonWriter;
        this.eventStreamBinaryWriter = eventStreamBinaryWriter;
    }

    public EventStreamWriter get(final StreamContentType streamContentType) {
        switch (streamContentType) {
            case JSON:
                return eventStreamJsonWriter;
            case BINARY:
                return eventStreamBinaryWriter;
            default:
                throw new InternalNakadiException(String.format(
                        "failed to find stream content type %s", streamContentType));
        }
    }
}
