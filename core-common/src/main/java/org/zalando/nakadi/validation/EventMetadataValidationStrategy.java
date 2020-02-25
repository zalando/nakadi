package org.zalando.nakadi.validation;

import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventType;

@Component
public class EventMetadataValidationStrategy extends ValidationStrategy {

    @Override
    public EventValidator materialize(final EventType eventType) {
        return new MetadataValidator();
    }
}
