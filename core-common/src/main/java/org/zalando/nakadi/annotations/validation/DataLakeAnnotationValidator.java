package org.zalando.nakadi.annotations.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;
import java.util.regex.Pattern;

public class DataLakeAnnotationValidator implements ConstraintValidator<DataLakeValidAnnotations, Map<String, String>> {
    private static final Pattern ANNOTATIONS_PERIOD_PATTERN = Pattern.compile(
            "^(unlimited|(([1-9]\\d{0,2}|[1-2]\\d{3}|3[0-5]\\d{2}|36[0-4]\\d|3650)(\\sdays?))|" +
                    "(([1-9]|[1-9]\\d|[1][01]\\d|120)(\\smonths?))|(([1-9]|(10))(\\syears?)))$");
    public static final String RETENTION_PERIOD_ANNOTATION = "datalake.zalando.org/retention-period";
    public static final String RETENTION_REASON_ANNOTATION = "datalake.zalando.org/retention-period-reason";

    @Override
    public boolean isValid(final Map<String, String> annotations, final ConstraintValidatorContext context) {
        if (annotations == null || annotations.size() == 0) {
            return true;
        }
        if (annotations.containsKey(RETENTION_PERIOD_ANNOTATION)) {
            if (annotations.getOrDefault(RETENTION_REASON_ANNOTATION, "").equals("")) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(
                                "Retention reason is required, when " + RETENTION_PERIOD_ANNOTATION + " is specified")
                        .addConstraintViolation();
                return false;
            }

            if (!ANNOTATIONS_PERIOD_PATTERN.matcher(annotations.get(RETENTION_PERIOD_ANNOTATION)).find()) {
                context.disableDefaultConstraintViolation();
                context.buildConstraintViolationWithTemplate(RETENTION_PERIOD_ANNOTATION +
                                " does not comply with pattern: " + ANNOTATIONS_PERIOD_PATTERN)
                        .addConstraintViolation();
                return false;
            }
        }
        return true;
    }
}
