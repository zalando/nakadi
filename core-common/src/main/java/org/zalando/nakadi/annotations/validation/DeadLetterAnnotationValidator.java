package org.zalando.nakadi.annotations.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;

public class DeadLetterAnnotationValidator implements
        ConstraintValidator<DeadLetterValidAnnotations, Map<String, String>> {

    public static final String SUBSCRIPTION_MAX_EVENT_SEND_COUNT =
            "nakadi.zalando.org/subscription-max-event-send-count";

    @Override
    public boolean isValid(final Map<String, String> annotations, final ConstraintValidatorContext context) {
        if (annotations == null) {
            return true;
        }

        final String failedCommitCount = annotations.get(SUBSCRIPTION_MAX_EVENT_SEND_COUNT);
        if (failedCommitCount == null) {
            return true;
        }

        final Integer commits;
        try {
            commits = Integer.valueOf(failedCommitCount);
        } catch (final NumberFormatException e) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate(SUBSCRIPTION_MAX_EVENT_SEND_COUNT + " must be an integer")
                    .addConstraintViolation();
            return false;
        }

        if (commits < 2 || commits > 10) {
            context.disableDefaultConstraintViolation();
            context.buildConstraintViolationWithTemplate(
                    SUBSCRIPTION_MAX_EVENT_SEND_COUNT + " must be between 2 and 10")
                    .addConstraintViolation();
            return false;
        }

        return true;
    }

}
