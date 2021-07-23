package org.zalando.nakadi.annotations.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({
        ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE,
        ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
@Size(max = 1000)
@Pattern(regexp = "^([a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9])?)?$", message = "Should contain only allowed symbols")
@Constraint(validatedBy = {})
public @interface LabelValue {
    String message() default "{org.zalando.nakadi.annotations.validation.LabelValue.message}}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
