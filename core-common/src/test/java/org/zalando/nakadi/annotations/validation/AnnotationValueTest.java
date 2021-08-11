package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AnnotationValueTest {
    public static class TestClass {
        @Valid
        @AnnotationValue
        private String keyValue;

        public TestClass(final String keyValue) {
            this.keyValue = keyValue;
        }
    }

    private Validator validator;

    @Before
    public void prepareValidator() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    public void testLongAnnotationValue() {
        final String longerAnnotation = "a".repeat(1001);
        assertFalse(validator.validate(new TestClass(longerAnnotation)).isEmpty());
    }

    @Test
    public void testNullAnnotationValue() {
        assertTrue(validator.validate(new TestClass(null)).isEmpty());
    }

    @Test
    public void testValidAnnotationValue() {
        assertTrue(validator.validate(new TestClass("sdfg")).isEmpty());
        assertTrue(validator.validate(new TestClass("{\"key\":\"value\"}")).isEmpty());
    }

}
