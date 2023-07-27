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
    public void whenMaterializationEventFormatIsWrongThenFail() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.MATERIALISE_EVENTS_ANNOTATION, "1 day"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue("When the format of the Materialize Event annotation is wrong, the name of the annotation " +
                        "should be present",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        DataLakeAnnotationValidator.MATERIALISE_EVENTS_ANNOTATION)));
    }

    @Test
    public void whenMaterializationEventIsOnNoRetentionPeriodFail() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.MATERIALISE_EVENTS_ANNOTATION, "on"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue("Annotation " + DataLakeAnnotationValidator.RETENTION_REASON_ANNOTATION + " is required," +
                        " when " + DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION + " is specified.",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        DataLakeAnnotationValidator.MATERIALISE_EVENTS_ANNOTATION)));
    }

    @Test
    public void whenRetentionPeriodThenRetentionReasonRequired() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION, "1 day"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue("When the retention period is specified but the retention reason is not," +
                        " the error message should include the retention reason annotation name",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        DataLakeAnnotationValidator.RETENTION_REASON_ANNOTATION)));
        assertTrue("When the retention period is specified but the retention reason is not," +
                        " the error message should include the retention period annotation name",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION)));
    }

    @Test
    public void whenRetentionPeriodFormatIsWrongThenFail() {
        final var annotations = Map.of(
                DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION, "1 airplane",
                DataLakeAnnotationValidator.RETENTION_REASON_ANNOTATION, "I need my data"
        );
        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));

        assertTrue("When retention period format is wrong, the message should contain a the annotation name",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        DataLakeAnnotationValidator.RETENTION_PERIOD_ANNOTATION)));
        assertTrue("When retention period format is wrong, the message should contain a link " +
                        "to the documentation",
                result.stream().anyMatch(r -> r.getMessage().contains(
                        "https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI")));
    }

    @Test
    public void whenRetentionPeriodAndReasonThenOk() {
        final String[] validRetentionPeriodValues = {
                "unlimited",
                "12 days",
                "3650 days",
                "120 months",
                "1 month",
                "10 years",
                "25d",
                "1m",
                "2y",
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
    public void whenMaterializationEventThenOk() {
        final String materialisationEventValue = "off";

        final var annotations = Map.of(
                DataLakeAnnotationValidator.MATERIALISE_EVENTS_ANNOTATION, materialisationEventValue
        );

        final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(annotations));
        assertTrue("Materialization event is off.", result.isEmpty());
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
