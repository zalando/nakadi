package org.zalando.nakadi.annotations.validation;

import org.junit.Before;
import org.junit.Test;

import javax.validation.ConstraintViolation;
import javax.validation.Valid;
import javax.validation.Validation;
import javax.validation.Validator;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class AnnotationKeyTest {
    public static class TestClass {
        @Valid
        @AnnotationKey
        private String keyValue;

        public TestClass(final String keyValue) {
            this.keyValue = keyValue;
        }
    }

    private Validator validator;

    // Something that is 63 characters long.
    private static final String LARGE_NAME = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-abcdefghijklmnopqrstuvwxyz";
    private static final String LARGE_NAME_LOW = LARGE_NAME.toLowerCase();
    // Something that is 253 characters long.
    private static final String LONG_PREFIX = LARGE_NAME_LOW + LARGE_NAME_LOW + "." + LARGE_NAME_LOW + LARGE_NAME_LOW;
    static final List<String> VALID_KEYS = Arrays.asList(
            "test-annotation",
            "nakadi.io/test-annotation",
            "nakadi-test.io/test-annotation",
            "nakadi-test/test-Annotation",
            "test/t-e_s.t",
            "test/1t-e_s.t0",
            LONG_PREFIX + "/" + LARGE_NAME
    );
    static final List<String> INVALID_KEYS = Arrays.asList(
            "", // Key cannot be empty
            "a/b/c", // Key should not have more than one prefix
            "/test", //Key prefix should not be empty
            "test/",//Key name should not be empty
            "test/_test", // Key name should start and end with [a-zA-Z0-9]
            "test/-test", // Key name should start and end with [a-zA-Z0-9]
            "test/.test", // Key name should start and end with [a-zA-Z0-9]
            "test/test_", // Key name should start and end with [a-zA-Z0-9]
            "test/test-", // Key name should start and end with [a-zA-Z0-9]
            "test/test.", // Key name should start and end with [a-zA-Z0-9]
            "test/te*st", // Key name cannot have special character except [-._]
            "test/te#st", // Key name cannot have special character except [-._]
            "test/" + LARGE_NAME + "z", // Key name cannot be more than 63 characters
            "A-test/test", // Prefix cannot have upper case
            ".test/test", // Prefix should be valid subdomain
            "test..com/test", // Prefix should be valid subdomain
            "test.-org/test", // Prefix should be valid subdomain
            LONG_PREFIX + "z/test" // Key prefix cannot be more than 253 characters
    );


    @Before
    public void prepareValidator() {
        validator = Validation.buildDefaultValidatorFactory().getValidator();
    }

    @Test
    public void whenValidAnnotationThenNoViolation() {
        for (final String validKey : VALID_KEYS) {
            final Set<ConstraintViolation<TestClass>> result = validator.validate(new TestClass(validKey));
            assertTrue("Valid key " + validKey + " treated as invalid", result.isEmpty());
        }
    }

    @Test
    public void whenInvalidAnnotationThanViolate() {
        for (final String invalidKey : INVALID_KEYS) {
            final Set<ConstraintViolation<TestClass>> violations = validator.validate(new TestClass(invalidKey));
            assertFalse(violations.isEmpty(), "Key " + invalidKey + " should be invalid");
        }
    }

}
