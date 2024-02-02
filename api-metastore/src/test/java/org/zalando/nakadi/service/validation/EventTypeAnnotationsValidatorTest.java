package org.zalando.nakadi.service.validation;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;

import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.FeatureToggleService;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventTypeAnnotationsValidatorTest {
    private static final String A_TEST_APPLICATION = "baz";

    private FeatureToggleService featureToggleService;
    private AuthorizationService authorizationService;
    private EventTypeAnnotationsValidator validator;

    @Before
    public void setUp() {
        featureToggleService = mock(FeatureToggleService.class);
        authorizationService = mock(AuthorizationService.class);
        validator = new EventTypeAnnotationsValidator(
            featureToggleService, authorizationService, Collections.singletonList(A_TEST_APPLICATION));
    }

    @Test
    public void whenMaterializationEventFormatIsWrongThenFail() {
        final var annotations = Map.of(
                EventTypeAnnotationsValidator.MATERIALISE_EVENTS_ANNOTATION, "1 day"
        );
        try {
            validator.validateAnnotations(annotations);
            Assert.fail("not reachable");
        } catch (InvalidEventTypeException e) {
            Assert.assertTrue(
                "When the format of the Materialize Event annotation is wrong, the name of the annotation " +
                "should be present",
                e.getMessage().contains(EventTypeAnnotationsValidator.MATERIALISE_EVENTS_ANNOTATION));
        }
    }

    @Test
    public void whenRetentionPeriodThenRetentionReasonRequired() {
        final var annotations = Map.of(
                EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION, "1 day"
        );
        try {
            validator.validateAnnotations(annotations);
            Assert.fail("not reachable");
        } catch (InvalidEventTypeException e) {
            Assert.assertTrue(
                "When the retention period is specified but the retention reason is not," +
                " the error message should include the retention reason annotation name",
                e.getMessage().contains(EventTypeAnnotationsValidator.RETENTION_REASON_ANNOTATION));
            Assert.assertTrue(
                "When the retention period is specified but the retention reason is not," +
                " the error message should include the retention period annotation name",
                e.getMessage().contains(EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION));
        }
    }

    @Test
    public void whenRetentionPeriodFormatIsWrongThenFail() {
        final var annotations = Map.of(
                EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION, "1 airplane",
                EventTypeAnnotationsValidator.RETENTION_REASON_ANNOTATION, "I need my data"
        );
        try {
            validator.validateAnnotations(annotations);
            Assert.fail("not reachable");
        } catch (InvalidEventTypeException e) {
            Assert.assertTrue(
                "When retention period format is wrong, the message should contain a the annotation name",
                e.getMessage().contains(EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION));
            Assert.assertTrue(
                "When retention period format is wrong, the message should contain a link to the documentation",
                e.getMessage().contains(
                        "https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI"));
        }
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
                    EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION, validRetentionPeriod,
                    EventTypeAnnotationsValidator.RETENTION_REASON_ANNOTATION, "I need my data"
            );

            validator.validateAnnotations(annotations);
        }
    }

    @Test
    public void whenMaterializationEventsThenOk() {
        final String[] validMaterialisationEventsValues = {"off", "on"};

        for (final var materialisationEventValue : validMaterialisationEventsValues) {
            final var annotations = Map.of(
                    EventTypeAnnotationsValidator.MATERIALISE_EVENTS_ANNOTATION, materialisationEventValue
                    EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION, "1m",
                    EventTypeAnnotationsValidator.RETENTION_REASON_ANNOTATION, "for testing"
            );

            validator.validateAnnotations(annotations);
        }
    }

    @Test
    public void whenDataLakeAnnotationsEnforcedThenMaterializationIsRequired() {
        when(featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)).thenReturn(true);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> A_TEST_APPLICATION));

        try {
            validator.validateAnnotations(Collections.emptyMap());
            Assert.fail("not reachable");
        } catch (InvalidEventTypeException e) {
            Assert.assertTrue(
                    e.getMessage().contains(EventTypeAnnotationsValidator.MATERIALISE_EVENTS_ANNOTATION));
        }
    }

    @Test
    public void whenMaterializationIsOnThenRetentionPeriodIsRequired() {
        try {
            validator.validateAnnotations(Collections.singletonMap(
                    EventTypeAnnotationsValidator.MATERIALISE_EVENTS_ANNOTATION, "on"));
            Assert.fail("not reachable");
        } catch (InvalidEventTypeException e) {
            Assert.assertTrue(
                    e.getMessage().contains(EventTypeAnnotationsValidator.RETENTION_PERIOD_ANNOTATION));
        }
    }

    @Test
    public void itWorksWithOtherAnnotations() {
        final var annotations = Map.of("some-annotation", "some-value");
        validator.validateAnnotations(annotations);
    }

    @Test
    public void itWorksWithoutAnnotations() {
        validator.validateAnnotations(null);
    }
}
