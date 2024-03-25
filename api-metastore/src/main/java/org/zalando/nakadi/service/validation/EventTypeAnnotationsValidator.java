package org.zalando.nakadi.service.validation;

import com.google.common.annotations.VisibleForTesting;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.Feature;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.service.FeatureToggleService;

import javax.validation.constraints.NotNull;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

@Component
public class EventTypeAnnotationsValidator {
    private static final Pattern DATA_LAKE_ANNOTATIONS_PERIOD_PATTERN = Pattern.compile(
            "^(unlimited|(([7-9]|[1-9]\\d{1,2}|[1-2]\\d{3}|3[0-5]\\d{2}|36[0-4]\\d|3650)((\\sdays?)|(d)))" +
                    "|(([1-9][0-9]?|[1-4][0-9]{2}|5([0-1][0-9]|2[0-1]))((\\sweeks?)|(w)))|" +
                    "(([1-9]|[1-9]\\d|[1][01]\\d|120)((\\smonths?)|(m)))|(([1-9]|(10))((\\syears?)|(y))))$");
    static final String DATA_LAKE_RETENTION_PERIOD_ANNOTATION = "datalake.zalando.org/retention-period";
    static final String DATA_LAKE_RETENTION_REASON_ANNOTATION = "datalake.zalando.org/retention-period-reason";
    static final String DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION = "datalake.zalando.org/materialize-events";

    private final FeatureToggleService featureToggleService;
    private final AuthorizationService authorizationService;
    private final List<String> enforcedAuthSubjects;

    @Autowired
    public EventTypeAnnotationsValidator(
            final FeatureToggleService featureToggleService,
            final AuthorizationService authorizationService,
            @Value("${nakadi.data_lake.annotations.enforced_auth_subjects:}") final List<String> enforcedAuthSubjects
    ) {
        this.featureToggleService = featureToggleService;
        this.authorizationService = authorizationService;
        this.enforcedAuthSubjects = enforcedAuthSubjects;
    }

    public void validateAnnotations(
            final EventTypeBase oldEventType,
            @NotNull final EventTypeBase newEventType)
            throws InvalidEventTypeException {

        final var oldAnnotations = oldEventType == null ?
                null : Optional.ofNullable(oldEventType.getAnnotations()).orElseGet(Collections::emptyMap);
        final var newAnnotations = Optional.ofNullable(newEventType.getAnnotations())
                .orElseGet(Collections::emptyMap);
        validateDataLakeAnnotations(oldAnnotations, newAnnotations);
    }

    @VisibleForTesting
    void validateDataLakeAnnotations(
            // null iff we're validating a new event type (i.e. there is no old event type)
            final Map<String, String> oldAnnotations,
            @NotNull final Map<String, String> annotations) {
        final var materializeEvents = annotations.get(DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION);
        final var retentionPeriod = annotations.get(DATA_LAKE_RETENTION_PERIOD_ANNOTATION);

        if (materializeEvents != null) {
            if (!materializeEvents.equals("off") && !materializeEvents.equals("on")) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION
                        + " is not valid. Provided value: \""
                        + materializeEvents
                        + "\". Possible values are: \"on\" or \"off\".");
            }
            if (materializeEvents.equals("on")) {
                if (retentionPeriod == null) {
                    throw new InvalidEventTypeException("Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                            + " is required, when " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION + " is \"on\".");
                }
            }
        }

        if (retentionPeriod != null) {
            final var retentionReason = annotations.get(DATA_LAKE_RETENTION_REASON_ANNOTATION);
            if (retentionReason == null || retentionReason.isEmpty()) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_RETENTION_REASON_ANNOTATION + " is required, when "
                        + DATA_LAKE_RETENTION_PERIOD_ANNOTATION + " is specified.");
            }

            if (!DATA_LAKE_ANNOTATIONS_PERIOD_PATTERN.matcher(retentionPeriod).find()) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_RETENTION_PERIOD_ANNOTATION
                        + " does not comply with regex. See documentation "
                        + "(https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI"
                        + "/edit#heading=h.kmvigbxbn1dj) for more details.");
            }
        }

        // Validation of @datalake.zalando.org/materialize-events is performed
        // only on new event-types or on event-types that have already migrated to using this new annotation.
        final var stricterCheck = (oldAnnotations == null
                || oldAnnotations.containsKey(DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION));
        if (stricterCheck && areDataLakeAnnotationsMandatory()) {
            if (materializeEvents == null) {
                throw new InvalidEventTypeException(
                        "Annotation " + DATA_LAKE_MATERIALIZE_EVENTS_ANNOTATION + " is required");
            }
        }
    }

    private boolean areDataLakeAnnotationsMandatory() {
        if (!featureToggleService.isFeatureEnabled(Feature.FORCE_DATA_LAKE_ANNOTATIONS)) {
            return false;
        }
        if (enforcedAuthSubjects.contains("*")) {
            return true;
        }

        final var subject = authorizationService.getSubject().map(Subject::getName).orElse("");
        return enforcedAuthSubjects.contains(subject);
    }
}
