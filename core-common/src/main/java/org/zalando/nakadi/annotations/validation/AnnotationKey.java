package org.zalando.nakadi.annotations.validation;

import javax.validation.Constraint;
import javax.validation.Payload;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.Pattern;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({
        ElementType.FIELD, ElementType.METHOD, ElementType.PARAMETER, ElementType.ANNOTATION_TYPE,
        ElementType.TYPE_USE})
@Retention(RetentionPolicy.RUNTIME)
@NotEmpty(message = "Key cannot be empty")
@Pattern(regexp = "^([a-z0-9]([-a-z0-9]*[a-z0-9])?(\\.[a-z0-9]([-a-z0-9]*[a-z0-9])?)*/)?[^/]*$",
        message = "Key name should be an optional lowercase domain name with path")
@Pattern(regexp = "^([-.a-z0-9]{0,253}/)?[^/]*$",
        message = "Key prefix cannot be more than 253 characters.")
@Pattern(regexp = "^([-.a-z0-9]+/)?[^/]{0,63}$",
        message = "Key name cannot be more than 63 characters.")
@Pattern(regexp = "^([^/]+/)?[a-zA-Z0-9]([a-zA-Z0-9_.-]*[a-zA-Z0-9])?$",
        message = "Key name should start and end with a letter or a digit")
@Constraint(validatedBy = {})
public @interface AnnotationKey {
    String message() default "{org.zalando.nakadi.annotations.validation.AnnotationKey.message}";

    Class<?>[] groups() default {};

    Class<? extends Payload>[] payload() default {};
}
