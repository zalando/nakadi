package org.zalando.nakadi.annotations.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;
import java.util.regex.Pattern;

public class DataLakeAnnotationValidator implements ConstraintValidator<DataLakeValidAnnotations, Map<String, String>> {
    private static final Pattern ANNOTATIONS_PERIOD_PATTERN = Pattern.compile(
            "^(unlimited|(([7-9]|[1-9]\\d{1,2}|[1-2]\\d{3}|3[0-5]\\d{2}|36[0-4]\\d|3650)((\\sdays?)|(d)))" +
                    "|(([1-9][0-9]?|[1-4][0-9]{2}|5([0-1][0-9]|2[0-1]))((\\sweeks?)|(w)))|" +
                    "(([1-9]|[1-9]\\d|[1][01]\\d|120)((\\smonths?)|(m)))|(([1-9]|(10))((\\syears?)|(y))))$");
    public static final String RETENTION_PERIOD_ANNOTATION = "datalake.zalando.org/retention-period";
    public static final String RETENTION_REASON_ANNOTATION = "datalake.zalando.org/retention-period-reason";
    public static final String MATERIALISE_EVENTS_ANNOTATION = "datalake.zalando.org/materialize-events";

    @Override
    public boolean isValid(final Map<String, String> annotations, final ConstraintValidatorContext context) {
        if (annotations == null || annotations.size() == 0) {
            return true;
        }
        String materialiseEventsAnnotation = annotations.get(MATERIALISE_EVENTS_ANNOTATION);
        if (annotations.containsKey(MATERIALISE_EVENTS_ANNOTATION)) {
            if (!materialiseEventsAnnotation.equals("off") &&
                    !materialiseEventsAnnotation.equals("on")) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate("Annotation " + MATERIALISE_EVENTS_ANNOTATION
                                + " is not valid. Provided value: \""
                                + materialiseEventsAnnotation
                                + "\". Possible values are: \"on\" or \"off\".")
                        .addConstraintViolation();
                return false;
            }
            if (materialiseEventsAnnotation.equals("on") &&
                    !annotations.containsKey(RETENTION_PERIOD_ANNOTATION)) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate("Annotation " + RETENTION_PERIOD_ANNOTATION
                                + " is required, when "
                                + MATERIALISE_EVENTS_ANNOTATION + " with value: \""
                                + materialiseEventsAnnotation
                                + "\" is specified.")
                        .addConstraintViolation();
                return false;
            }
        }

        if (annotations.containsKey(RETENTION_PERIOD_ANNOTATION)) {
            if (annotations.getOrDefault(RETENTION_REASON_ANNOTATION, "").equals("")) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(
                                "Annotation " + RETENTION_REASON_ANNOTATION + " is required, when "
                                        + RETENTION_PERIOD_ANNOTATION + " is specified.")
                        .addConstraintViolation();
                return false;
            }

            if (!ANNOTATIONS_PERIOD_PATTERN.matcher(annotations.get(RETENTION_PERIOD_ANNOTATION)).find()) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate("Annotation " + RETENTION_PERIOD_ANNOTATION +
                                " does not comply with regex. See documentation " +
                                "(https://docs.google.com/document/d/1-SwwpwUqauc_pXu-743YA1gO8l5_R_Gf4nb" +
                                "Yml1ySiI/edit#heading=h.kmvigbxbn1dj) for more details.")
                        .addConstraintViolation();
                return false;
            }
        }
        return true;
    }
}
