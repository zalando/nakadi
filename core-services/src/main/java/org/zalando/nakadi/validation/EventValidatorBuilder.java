package org.zalando.nakadi.validation;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventCategory;
import org.zalando.nakadi.domain.EventType;

import java.util.ArrayList;
import java.util.List;

@Component
public class EventValidatorBuilder {

    private final EventBodyMustRespectSchema schemaRespectValidationStrategy;
    private final EventMetadataValidationStrategy metadataValidationStrategy;

    @Autowired
    public EventValidatorBuilder(
            final EventBodyMustRespectSchema schemaRespectValidationStrategy,
            final EventMetadataValidationStrategy metadataValidationStrategy) {
        this.schemaRespectValidationStrategy = schemaRespectValidationStrategy;
        this.metadataValidationStrategy = metadataValidationStrategy;
    }

    public EventTypeValidator build(final EventType eventType) {
        final List<EventValidator> validatorList = new ArrayList<>();
        validatorList.add(schemaRespectValidationStrategy.materialize(eventType));
        if (eventType.getCategory() == EventCategory.BUSINESS || eventType.getCategory() == EventCategory.DATA) {
            validatorList.add(metadataValidationStrategy.materialize(eventType));
        }
        return new EventTypeValidator(validatorList);
    }
}

