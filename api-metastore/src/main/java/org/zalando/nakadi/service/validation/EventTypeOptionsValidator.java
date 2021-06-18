package org.zalando.nakadi.service.validation;

import org.zalando.nakadi.domain.EventTypeOptions;
import org.zalando.nakadi.exceptions.runtime.EventTypeOptionsValidationException;

public final class EventTypeOptionsValidator {

    private final long minTopicRetentionMs;
    private final long maxTopicRetentionMs;

    public EventTypeOptionsValidator(final long minTopicRetentionMs,
                                     final long maxTopicRetentionMs) {
        this.minTopicRetentionMs = minTopicRetentionMs;
        this.maxTopicRetentionMs = maxTopicRetentionMs;
    }

    public void checkRetentionTime(final EventTypeOptions options) throws EventTypeOptionsValidationException {
        if (options == null) {
            return;
        }

        final Long retentionTime = options.getRetentionTime();
        if (retentionTime != null) {
            if (retentionTime > maxTopicRetentionMs) {
                throw new EventTypeOptionsValidationException(
                        "Field \"options.retention_time\" can not be more than " + maxTopicRetentionMs);
            } else if (retentionTime < minTopicRetentionMs) {
                throw new EventTypeOptionsValidationException(
                        "Field \"options.retention_time\" can not be less than " + minTopicRetentionMs);
            }
        }
    }

}
