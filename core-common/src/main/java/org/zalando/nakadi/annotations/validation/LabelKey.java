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
@AnnotationKey
@Constraint(validatedBy = {})
public @interface LabelKey {
    String message() default "{org.zalando.nakadi.annotations.validation.LabelKey.message}}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
