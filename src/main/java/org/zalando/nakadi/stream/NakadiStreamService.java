package org.zalando.nakadi.stream;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.stream.expression.Interpreter;

import java.util.stream.Stream;

@Component
public class NakadiStreamService {

    private final EventTypeRepository eventTypeRepository;
    private final KafkaStreamConfig kafkaStreamConfig;

    @Autowired
    public NakadiStreamService(final EventTypeRepository eventTypeRepository,
                               final KafkaStreamConfig kafkaStreamConfig) {
        this.eventTypeRepository = eventTypeRepository;
        this.kafkaStreamConfig = kafkaStreamConfig;
    }

    public void stream(final StreamConfig streamConfig) {

        final Interpreter interpreter = new Interpreter();
        interpreter.expr(streamConfig.getExpressions());

        final Stream<EventType> eventTypeStream = eventTypeRepository.list().stream();
        streamConfig.setTopics(eventTypeStream
                .filter(eventType -> streamConfig.getEventTypes().contains(eventType.getName()))
                .map(EventType::getTopic)
                .toArray(String[]::new));

        if (streamConfig.getTopics().length == 0)
            throw new RuntimeException("Event types were not found");

        if (streamConfig.isToEventType()) {
            eventTypeStream
                    .filter(eventType -> streamConfig.getOutputEventType().equals(eventType.getName()))
                    .findFirst()
                    .map(eventType -> streamConfig.setOutputTopic(eventType.getTopic()))
                    .orElseThrow(() -> new RuntimeException("Output event type does nt exist"));
        }

        streamConfig.setKafkaStreamProperties(kafkaStreamConfig.getProperties(streamConfig.hashCode()));

        NakadiStreamFactory.create().stream(streamConfig, interpreter);
    }
}
