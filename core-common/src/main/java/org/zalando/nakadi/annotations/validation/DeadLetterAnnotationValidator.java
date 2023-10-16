package org.zalando.nakadi.annotations.validation;

import org.zalando.nakadi.domain.UnprocessableEventPolicy;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;

public class DeadLetterAnnotationValidator implements
        ConstraintValidator<DeadLetterValidAnnotations, Map<String, String>> {

    public static final String SUBSCRIPTION_MAX_EVENT_SEND_COUNT =
            "nakadi.zalando.org/subscription-max-event-send-count";

    public static final String SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY =
            "nakadi.zalando.org/subscription-unprocessable-event-policy";

    @Override
    public boolean isValid(final Map<String, String> annotations, final ConstraintValidatorContext context) {
        if (annotations == null) {
            return true;
        }
        if (!isValidMaxEventSendCount(annotations, context)) {
            return false;
        }
        if (!isValidUnprocessableEventPolicy(annotations, context)) {
            return false;
        }
        return true;
    }

    private boolean isValidMaxEventSendCount(
            final Map<String, String> annotations, final ConstraintValidatorContext context) {

        final String maxEventSendCount = annotations.get(SUBSCRIPTION_MAX_EVENT_SEND_COUNT);
        if (maxEventSendCount == null) {
            return true;
        }

        final Integer commits;
        try {
            commits = Integer.valueOf(maxEventSendCount);
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

        final String unprocessableEventPolicy = annotations.get(SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY);
        if (unprocessableEventPolicy == null) {
            context.disableDefaultConstraintViolation();
            context
                    .buildConstraintViolationWithTemplate(
                            SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY + " must be also set when setting " +
                            SUBSCRIPTION_MAX_EVENT_SEND_COUNT)
                    .addConstraintViolation();
            return false;
        }

        return true;
    }

    private boolean isValidUnprocessableEventPolicy(
            final Map<String, String> annotations, final ConstraintValidatorContext context) {

        final String unprocessableEventPolicy = annotations.get(SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY);
        if (unprocessableEventPolicy == null) {
            return true;
        }

        try {
            UnprocessableEventPolicy.valueOf(unprocessableEventPolicy);
        } catch (final IllegalArgumentException e) {
            context.disableDefaultConstraintViolation();
            context
                    .buildConstraintViolationWithTemplate(
                            SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY + " must be one of: SKIP_EVENT, DEAD_LETTER_QUEUE")
                    .addConstraintViolation();
            return false;
        }

        final String maxEventSendCount = annotations.get(SUBSCRIPTION_MAX_EVENT_SEND_COUNT);
        if (maxEventSendCount == null) {
            context.disableDefaultConstraintViolation();
            context
                    .buildConstraintViolationWithTemplate(
                            SUBSCRIPTION_MAX_EVENT_SEND_COUNT + " must be also set when setting " +
                            SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY)
                    .addConstraintViolation();
            return false;
        }

        return true;
    }
}
