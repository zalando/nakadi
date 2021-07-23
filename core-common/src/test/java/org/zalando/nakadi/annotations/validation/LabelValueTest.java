package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Test;

import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;

import static org.junit.Assert.assertTrue;
import static org.springframework.test.util.AssertionErrors.assertFalse;

public class LabelValueTest {
    public static class TestClass {
        @Valid
        @LabelValue
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
    public void whenValidLabelValueThenNoViolation() {
        final String[] validValues = new String[]{
                "",
                null,
                "a".repeat(1000)
        };

        for (final String valid : validValues) {
            assertTrue("Valid value '" + valid + "' treated as invalid",
                    validator.validate(new TestClass(valid)).isEmpty());
        }
    }

    @Test
    public void whenInvalidLabelValueThenViolation() {
        final String[] invalidValues = new String[]{
                "a".repeat(1001),
                "-test",
                ".test",
                "_test",
                "te*st",
                "te$st",
                "test_",
                "test.",
                "test-"
        };

        for (final String invalid : invalidValues) {
            assertFalse("Valid value '" + invalid + "' treated as valid",
                    validator.validate(new TestClass(invalid)).isEmpty());
        }

    }

}
