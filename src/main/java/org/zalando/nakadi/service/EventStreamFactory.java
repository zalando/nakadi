package org.zalando.nakadi.service;

import java.io.OutputStream;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.InvalidCursorException;
import org.zalando.nakadi.exceptions.NakadiException;
import org.zalando.nakadi.repository.EventConsumer;
import org.zalando.nakadi.repository.TopicRepository;

@Component
public class EventStreamFactory {

    private final TopicRepository topicRepository;

    @Autowired
    public EventStreamFactory(final TopicRepository topicRepository) {
        this.topicRepository = topicRepository;
    }

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
