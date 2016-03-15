package de.zalando.aruha.nakadi.validation;

import de.zalando.aruha.nakadi.domain.EventType;
import de.zalando.aruha.nakadi.domain.ValidationStrategyConfiguration;

public class EventMetadataValidationStrategy extends ValidationStrategy {
    public static final String NAME = "metadata-validation";

    @Override
    public EventValidator materialize(final EventType eventType, final ValidationStrategyConfiguration vsc) {
        return new MetadataValidator();
    }
}
