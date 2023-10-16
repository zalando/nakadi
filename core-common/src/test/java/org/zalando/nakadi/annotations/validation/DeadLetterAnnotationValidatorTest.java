package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Assert;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Collections;
import java.util.Map;

public class DeadLetterAnnotationValidatorTest {
    public static class TestClass {
        @Valid
        @DeadLetterValidAnnotations
        private final Map<
                @Valid @AnnotationKey String,
                @Valid @AnnotationValue String> annotations;

        public TestClass(final Map<String, String> annotations) {
            this.annotations = annotations;
        }
    }

    private Validator validator;

    @Before
    public void prepareValidator() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    public void whenEmptyAnnotationsThenOK() {
        Assert.assertTrue("No annotations is OK",
                validator.validate(new TestClass(null)).isEmpty());

        Assert.assertTrue("Empty annotations is OK",
                validator.validate(new TestClass(Collections.emptyMap())).isEmpty());
    }

    @Test
    public void whenValidMaxEventSendCountThenOK() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "3",
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "SKIP_EVENT");

        Assert.assertTrue("Valid max event send count is OK",
                validator.validate(new TestClass(annotations)).isEmpty());
    }

    @Test
    public void whenInvalidMaxEventSendCountThenFail() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "not-a-number",
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "SKIP_EVENT");

        Assert.assertTrue("Invalid max event send count is rejected",
                validator.validate(new TestClass(annotations)).stream().anyMatch(
                        r -> r.getMessage().contains(
                                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT)));
    }

    @Test
    public void whenTooLowMaxEventSendCountThenFail() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "1",
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "SKIP_EVENT");

        Assert.assertTrue("Too low max event send count is rejected",
                validator.validate(new TestClass(annotations)).stream().anyMatch(
                        r -> r.getMessage().contains(
                                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT)));
    }

    @Test
    public void whenTooHighMaxEventSendCountThenFail() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "11",
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "SKIP_EVENT");

        Assert.assertTrue("Too high max event send count is rejected",
                validator.validate(new TestClass(annotations)).stream().anyMatch(
                        r -> r.getMessage().contains(
                                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT)));
    }

    @Test
    public void whenExplicitlySetMaxEventSendCountSetAndNoPolicyThenFail() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "3");

        Assert.assertTrue("Unprocessable event policy must be set when setting max event send count",
                validator.validate(new TestClass(annotations)).stream()
                .map(ConstraintViolation::getMessage)
                .anyMatch(m ->
                        m.contains(DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT) &&
                        m.contains(DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY)));
    }

    @Test
    public void whenSkipEventPolicyThenOK() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "SKIP_EVENT",
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "3");

        Assert.assertTrue("Skip event policy is OK",
                validator.validate(new TestClass(annotations)).isEmpty());
    }

    @Test
    public void whenDeadLetterQueuePolicyThenOK() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "DEAD_LETTER_QUEUE",
                DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT, "3");

        Assert.assertTrue("Dead letter queue policy is OK",
                validator.validate(new TestClass(annotations)).isEmpty());
    }

    @Test
    public void whenExplicitlySetPolicyAndNoMaxEventSendCountSetThenFail() {
        final Map<String, String> annotations = Map.of(
                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "DEAD_LETTER_QUEUE");

        Assert.assertTrue("Max event send count must be set when setting unprocessable event policy",
                validator.validate(new TestClass(annotations)).stream()
                .map(ConstraintViolation::getMessage)
                .anyMatch(m ->
                        m.contains(DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY) &&
                        m.contains(DeadLetterAnnotationValidator.SUBSCRIPTION_MAX_EVENT_SEND_COUNT)));
    }

    @Test
    public void whenUnknownPolicyThenFail() {
        final Map<String, String> annotations =
                Map.of(DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY, "unknown");

        Assert.assertTrue("Unknown unprocessable event policy is rejected",
                validator.validate(new TestClass(annotations)).stream().anyMatch(
                        r -> r.getMessage().contains(
                                DeadLetterAnnotationValidator.SUBSCRIPTION_UNPROCESSABLE_EVENT_POLICY)));
    }
}
