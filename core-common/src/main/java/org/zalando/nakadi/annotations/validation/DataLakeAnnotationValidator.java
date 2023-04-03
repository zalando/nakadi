package org.zalando.nakadi.annotations.validation;

import javax.validation.ConstraintValidator;
import javax.validation.ConstraintValidatorContext;
import java.util.Map;
import java.util.regex.Pattern;

public class DataLakeAnnotationValidator implements ConstraintValidator<DataLakeValidAnnotations, Map<String, String>> {
    final private static Pattern AnnotationsPeriodPattern = Pattern.compile("^(unlimited|(([1-9]\\d{0,2}|[1-2]\\d{3}|3[0-5]\\d{2}|36[0-4]\\d|3650)(\\sdays?))|(([1-9]|[1-9]\\d|[1][01]\\d|120)(\\smonths?))|(([1-9]|(10))(\\syears?)))$");
    final public static String RetentionPeriodAnnotation = "datalake.zalando.org/retention-period";
    final public static String RetentionReasonAnnotation = "datalake.zalando.org/retention-period-reason";

    @Override
    public void initialize(DataLakeValidAnnotations constraintAnnotation) {
        ConstraintValidator.super.initialize(constraintAnnotation);
    }

    @Override
    public boolean isValid(Map<String, String> annotations, ConstraintValidatorContext context) {
        if (annotations.containsKey(RetentionPeriodAnnotation)) {
            if (annotations.getOrDefault(RetentionReasonAnnotation, "").equals("")) {
                return false;
            }

            return AnnotationsPeriodPattern.matcher(annotations.get(RetentionPeriodAnnotation)).find();
        }
        return false;
    }
}
