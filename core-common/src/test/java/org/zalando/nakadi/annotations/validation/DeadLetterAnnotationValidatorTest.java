package org.zalando.nakadi.annotations.validation;

import org.junit.Assert;
import org.junit.Test;

public class DeadLetterAnnotationValidatorTest {

    @Test
    public void testIsValid() {
        final DeadLetterAnnotationValidator deadLetterAnnotationValidator = new DeadLetterAnnotationValidator();
        Assert.assertTrue(deadLetterAnnotationValidator.isValid(null, null));
    }

}