package org.zalando.nakadi.validation;

import org.zalando.nakadi.domain.EventType;

public abstract class ValidationStrategy {

    public abstract EventValidator materialize(EventType eventType);
}
