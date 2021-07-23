package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LabelKeyTest {
    public static class TestClass {
        @Valid
        @LabelKey
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
    public void whenValidLabelKeyThenNoViolation() {
        for (final String validKey : AnnotationKeyTest.VALID_KEYS) {
            assertTrue("Valid key " + validKey + " treated as invalid",
                    validator.validate(new TestClass(validKey)).isEmpty());
        }
    }

    @Test
    public void whenInvalidLabelKeyThanViolate() {
        for (final String invalidKey : AnnotationKeyTest.INVALID_KEYS) {
            assertFalse("Key " + invalidKey + " should be invalid",
                    validator.validate(new AnnotationKeyTest.TestClass(invalidKey)).isEmpty());
        }
    }

}
