package org.zalando.nakadi.validation;

import org.springframework.validation.Errors;
import org.springframework.validation.Validator;
import org.zalando.nakadi.domain.EventTypeOptions;

public final class EventTypeOptionsValidator implements Validator {

    private final long minTopicRetentionMs;
    private final long maxTopicRetentionMs;

    public EventTypeOptionsValidator(final long minTopicRetentionMs,
                                     final long maxTopicRetentionMs) {
        this.minTopicRetentionMs = minTopicRetentionMs;
        this.maxTopicRetentionMs = maxTopicRetentionMs;
    }

    @Override
    public boolean supports(final Class<?> clazz) {
        return EventTypeOptions.class.equals(clazz);
    }

    @Override
    public void validate(final Object target, final Errors errors) {
        final EventTypeOptions options = (EventTypeOptions) target;
        checkRetentionTime(errors, options);
    }

    private void checkRetentionTime(final Errors errors, final EventTypeOptions options) {
        if (options == null) {
            return;
        }

        final Long retentionTime = options.getRetentionTime();
        if (retentionTime != null) {
            if (retentionTime > maxTopicRetentionMs) {
                createError(errors, "can not be more than " + maxTopicRetentionMs);
            } else if (retentionTime < minTopicRetentionMs) {
                createError(errors, "can not be less than " + minTopicRetentionMs);
            }
        }
    }

    private void createError(final Errors errors, final String message) {
        errors.rejectValue("options.retentionTime", null, message);
    }
}
