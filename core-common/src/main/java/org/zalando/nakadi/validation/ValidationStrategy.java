package org.zalando.nakadi.validation;

import org.zalando.nakadi.domain.EventType;

import javax.annotation.Nullable;

public interface ValidationStrategy {

    @Nullable
    EventValidator createValidator(EventType eventType);
}
