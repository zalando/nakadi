package org.zalando.nakadi.validation;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.ResourceAnnotations;
import org.zalando.nakadi.domain.ResourceLabels;
import org.zalando.nakadi.exceptions.runtime.InvalidResourceAnnotationException;
import org.zalando.nakadi.exceptions.runtime.InvalidResourceLabelException;

import java.util.Arrays;
import java.util.List;

public class ValidationHelperServiceTest {

    // Something that is 63 characters long.
    private static final String LARGE_NAME = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-abcdefghijklmnopqrstuvwxyz";
    private static final String LARGE_NAME_LOW = LARGE_NAME.toLowerCase();
    // Something that is 253 characters long.
    private static final String LONG_PREFIX = LARGE_NAME_LOW + LARGE_NAME_LOW + "." + LARGE_NAME_LOW + LARGE_NAME_LOW;
    private static final List<String> INVALID_KEYS = Arrays.asList(
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

    private static final List<String> VALID_KEYS = Arrays.asList(
            "test-annotation",
            "nakadi.io/test-annotation",
            "nakadi-test.io/test-annotation",
            "nakadi-test/test-Annotation",
            "test/t-e_s.t",
            "test/1t-e_s.t0",
            LONG_PREFIX + "/" + LARGE_NAME
    );

    private final ResourceValidationHelperService validationHelperService = new ResourceValidationHelperService();

    @Test
    public void testCheckAnnotationPassForValidAnnotations() {
        validationHelperService.checkAnnotations(null);
        final ResourceAnnotations annotations = new ResourceAnnotations();
        for (final String validKey : VALID_KEYS) {
            annotations.put(validKey, "testValue");
        }
        try {
            validationHelperService.checkAnnotations(annotations);
        } catch (final InvalidResourceAnnotationException irae) {
            Assert.fail("Validation failed with message: " + irae.getMessage());
        }
    }

    @Test
    public void testCheckAnnotationFailsForInvalidAnnotationKeys() {
        for (final String invalidKey : INVALID_KEYS) {
            final ResourceAnnotations annotations = new ResourceAnnotations();
            annotations.put(invalidKey, "invalidAnnotation");
            Assert.assertThrows(
                    "Invalid key " + invalidKey + " passed validation",
                    InvalidResourceAnnotationException.class,
                    () -> validationHelperService.checkAnnotations(annotations));
        }
    }

    @Test
    public void testCheckAnnotationFailsForLongerAnnotationValue() {
        final ResourceAnnotations annotations = new ResourceAnnotations();
        final String longerAnnotation = "a".repeat(ResourceValidationHelperService.MAX_ANNOTATION_LENGTH + 1);
        annotations.put("nakadi.io/test", longerAnnotation);
        Assert.assertThrows(
                "Invalid annotation passed validation",
                InvalidResourceAnnotationException.class,
                () -> validationHelperService.checkAnnotations(annotations));
    }

    @Test
    public void testCheckAnnotationPassForValidAnnotationValues() {
        final ResourceAnnotations annotations = new ResourceAnnotations();
        final String longerAnnotation = "a".repeat(ResourceValidationHelperService.MAX_ANNOTATION_LENGTH);
        annotations.put("nakadi.io/test-long", longerAnnotation);
        annotations.put("nakadi.io/test-null", null);
        annotations.put("nakadi.io/test-empty", "");
        annotations.put("nakadi.io/test-encoded-json", "{\"key\":\"value\"}");
        annotations.put("nakadi.io/test-simple-word", "test");
        try {
            validationHelperService.checkAnnotations(annotations);
        } catch (final InvalidResourceAnnotationException irae) {
            Assert.fail("Valid annotation value rejected. " + irae.getMessage());
        }
    }

    @Test
    public void testCheckLabelPassForValidLabels() {
        validationHelperService.checkLabels(null);
        final ResourceLabels labels = new ResourceLabels();
        for (final String validKey : VALID_KEYS) {
            labels.put(validKey, "testValue");
        }
        labels.put("nakadi.io/test-empty", "");
        labels.put("nakadi.io/test-null", null);
        labels.put("nakadi.io/test-long", "a".repeat(ResourceValidationHelperService.MAX_LABEL_LENGTH));
        try {
            validationHelperService.checkLabels(labels);
        } catch (final InvalidResourceLabelException irle) {
            Assert.fail("Validation failed with message" + irle.getMessage());
        }
    }

    @Test
    public void testCheckLabelFailsForInvalidLabelKeys() {
        for (final String invalidKey : INVALID_KEYS) {
            final ResourceLabels labels = new ResourceLabels();
            labels.put(invalidKey, "invalidLabel");
            Assert.assertThrows(
                    "Invalid key " + invalidKey + " passed validation",
                    InvalidResourceLabelException.class,
                    () -> validationHelperService.checkLabels(labels));
        }
    }

    @Test
    public void testCheckLabelFailsForInvalidLabelValues() {
        final List<String> invalidValues = Arrays.asList(
                "a".repeat(ResourceValidationHelperService.MAX_LABEL_LENGTH + 1),
                "-test",
                ".test",
                "_test",
                "te*st",
                "te$st",
                "test_",
                "test.",
                "test-"
        );
        for (final String invalidLabelValue : invalidValues) {
            final ResourceLabels labels = new ResourceLabels();
            labels.put("nakadi.io/test-label", invalidLabelValue);
            Assert.assertThrows(
                    "Invalid Label value accepted for " + invalidLabelValue,
                    InvalidResourceLabelException.class, () -> validationHelperService.checkLabels(labels));
        }
    }

}