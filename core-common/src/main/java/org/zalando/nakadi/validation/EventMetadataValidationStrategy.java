package org.zalando.nakadi.validation;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.ValidationStrategyConfiguration;

public class EventMetadataValidationStrategy extends ValidationStrategy {
    public static final String NAME = "metadata-validation";

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {
        return new MetadataValidator();
    }
}
