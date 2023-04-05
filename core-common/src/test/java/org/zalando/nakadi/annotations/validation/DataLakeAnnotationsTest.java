package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.assertTrue;

public class DataLakeAnnotationsTest {
    public static class TestClass {
        @Valid
        @DataLakeValidAnnotations
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
    public void whenRetentionPeriodThenRetentionReasonRequired() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION, "1 day"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue(result.stream().anyMatch(r -> r.getMessage().equals("Retention reason is required, when " +
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION + " is specified")));
    }

    @Test
    public void whenRetentionPeriodFormatIsWrongThenFail() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION, "1 airplane",
                DataLakeAnnotationValidator.RETENTION_REASON_ANNOTATION, "I need my data"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));

        assertTrue(result.stream().anyMatch(r -> r.getMessage().contains(
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION + " does not comply with pattern:")));
    }

    @Test
    public void whenRetentionPeriodAndReasonThenOk() {
        final String[] validRetentionPeriodValues = {
                "unlimited",
                "1 day",
                "2 days",
                "3650 days",
                "120 months",
                "1 month",
                "10 years",
                "1 year"
        };

        for (final String validRetentionPeriod : validRetentionPeriodValues) {
            final var annotations = Map.of(
                    DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION, validRetentionPeriod,
                    DataLakeAnnotationValidator.RETENTION_REASON_ANNOTATION, "I need my data"
            );

            final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
            assertTrue("Retention period and reason exist correctly", result.isEmpty());
        }
    }

    @Test
    public void itWorksWithOtherAnnotations() {
        final var annotations = Map.of("some-annotation", "some-value");
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue("Should not throw errors when few annotations are passed, " +
                "but none of them belongs to data lake", result.isEmpty());
    }

    @Test
    public void itWorksWithoutAnnotations() {
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(null));
        assertTrue("Should not throw errors when no annotation is passed", result.isEmpty());
    }
}
