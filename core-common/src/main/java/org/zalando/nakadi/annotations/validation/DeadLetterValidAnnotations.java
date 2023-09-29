package org.zalando.nakadi.annotations.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({
        ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE,
        ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
@Constraint(validatedBy = {DeadLetterAnnotationValidator.class})
public @interface DeadLetterValidAnnotations {
    String message() default "Auto DLQ params are not valid";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};

}
