package org.zalando.nakadi.service.validation;

import org.springframework.stereotype.Component;
import org.zalando.nakadi.exceptions.runtime.InvalidEventTypeException;

import java.util.Map;
import java.util.regex.Pattern;

@Component
public class EventTypeAnnotationsValidator {
    private static final Pattern ANNOTATIONS_PERIOD_PATTERN = Pattern.compile(
            "^(unlimited|(([7-9]|[1-9]\\d{1,2}|[1-2]\\d{3}|3[0-5]\\d{2}|36[0-4]\\d|3650)((\\sdays?)|(d)))" +
                    "|(([1-9][0-9]?|[1-4][0-9]{2}|5([0-1][0-9]|2[0-1]))((\\sweeks?)|(w)))|" +
                    "(([1-9]|[1-9]\\d|[1][01]\\d|120)((\\smonths?)|(m)))|(([1-9]|(10))((\\syears?)|(y))))$");
    static final String RETENTION_PERIOD_ANNOTATION = "datalake.zalando.org/retention-period";
    static final String RETENTION_REASON_ANNOTATION = "datalake.zalando.org/retention-period-reason";
    static final String MATERIALISE_EVENTS_ANNOTATION = "datalake.zalando.org/materialize-events";

    public void validateAnnotations(final Map<String, String> annotations) throws InvalidEventTypeException {
        validateDataLakeAnnotations(annotations);
    }

    private void validateDataLakeAnnotations(final Map<String, String> annotations) throws InvalidEventTypeException {
        if (annotations == null || annotations.size() == 0) {
            return;
        }

        final var materializeEvents = annotations.get(MATERIALISE_EVENTS_ANNOTATION);
        if (materializeEvents != null) {
            if (!materializeEvents.equals("off") && !materializeEvents.equals("on")) {
                throw new InvalidEventTypeException(
                        "Annotation " + MATERIALISE_EVENTS_ANNOTATION
                        + " is not valid. Provided value: \""
                        + materializeEvents
                        + "\". Possible values are: \"on\" or \"off\".");
            }
        }

        final var retentionPeriod = annotations.get(RETENTION_PERIOD_ANNOTATION);
        if (retentionPeriod != null) {
            final var retentionReason = annotations.get(RETENTION_REASON_ANNOTATION);
            if (retentionReason == null || retentionReason.isEmpty()) {
                throw new InvalidEventTypeException(
                        "Annotation " + RETENTION_REASON_ANNOTATION + " is required, when "
                        + RETENTION_PERIOD_ANNOTATION + " is specified.");
            }

            if (!ANNOTATIONS_PERIOD_PATTERN.matcher(retentionPeriod).find()) {
                throw new InvalidEventTypeException(
                        "Annotation " + RETENTION_PERIOD_ANNOTATION
                        + " does not comply with regex. See documentation "
                        + "(https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nbYml1ySiI"
                        + "/edit#heading=h.kmvigbxbn1dj) for more details.");
            }
        }
    }
}
