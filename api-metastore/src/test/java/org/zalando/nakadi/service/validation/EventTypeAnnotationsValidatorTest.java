package org.zalando.nakadi.service.validation;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.FeatureToggleService;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class EventTypeAnnotationsValidatorTest {
    private static final String A_TEST_APPLICATION = "baz";

    private FeatureToggleService featureToggleService;
    private AuthorizationService authorizationService;
    private EventTypeAnnotationsValidator validator;

    @BeforeEach
    public void setUp() {
        featureToggleService = mock(FeatureToggleService.class);
        authorizationService = mock(AuthorizationService.class);
        validator = new EventTypeAnnotationsValidator(
            featureToggleService, authorizationService, Collections.singletonList(A_TEST_APPLICATION));
    }

    @ParameterizedTest
    @MethodSource("getValidDataLakeAnnotations")
    public void testValidDataLakeAnnotations(final Map<String, String> annotations) {
        validator.validateDataLakeAnnotations(null, annotations);
    }

    public static Stream<Map<String, String>> getValidDataLakeAnnotations() {
        return Stream.of(
                Stream.of("off", "on")
                        .map(materialization -> Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, materialization,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1m",
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "for testing"
                        )),
                Stream.of(
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
                )
                        .map(retentionPeriod -> Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, retentionPeriod,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "I need my data"
                        )),
                Stream.of(
                        Map.of("some-annotation", "some-value")
                )
        ).flatMap(Function.identity());
    }

    @ParameterizedTest
    @MethodSource("getInvalidDataLakeAnnotations")
    public void testInvalidDataLakeAnnotations(final Map<String, String> annotations, final String[] errorMessages) {
        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataLakeAnnotations(null, annotations));
        Assertions.assertThat(exception.getMessage()).contains(errorMessages);
    }

    public static Stream<Arguments> getInvalidDataLakeAnnotations() {
        return Stream.of(
                Arguments.of(
                        // Invalid materialization value
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, "true"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Materialization is on without retention period
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION, "on"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Retention period without retention reason
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1 day"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION,
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                        }
                ),
                Arguments.of(
                        // Wrong retention period value
                        Map.of(
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION, "1 airplane",
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_REASON_ANNOTATION, "I need my data"
                        ),
                        new String[] {
                                EventTypeAnnotationsValidator.DATA_LAKE_RETENTION_PERIOD_ANNOTATION,
                                "https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI",
                        }
                )
        );
    }

    @Test
    public void whenDataLakeAnnotationsEnforcedThenMaterializationIsRequired() {
        when(featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)).thenReturn(true);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> A_TEST_APPLICATION));

        final var exception = assertThrows(
                InvalidEventTypeException.class,
                () -> validator.validateDataLakeAnnotations(null, Collections.emptyMap()));
        Assertions.assertThat(exception.getMessage())
                .contains(EventTypeAnnotationsValidator.DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION);
    }

    @Test
    public void testUpdateOfOldVersionWithoutDataLakeAnnotations() {
        when(featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)).thenReturn(true);
        when(authorizationService.getSubject()).thenReturn(Optional.of(() -> A_TEST_APPLICATION));

        validator.validateDataLakeAnnotations(Collections.emptyMap(), Collections.emptyMap());
    }
}
