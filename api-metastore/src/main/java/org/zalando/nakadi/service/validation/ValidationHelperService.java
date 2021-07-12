package org.zalando.nakadi.service.validation;

import org.springframework.stereotype.Component;
import org.zalando.nakadi.domain.ResourceAnnotations;
import org.zalando.nakadi.domain.ResourceLabels;
import org.zalando.nakadi.exceptions.runtime.InvalidResourceAnnotationException;
import org.zalando.nakadi.exceptions.runtime.InvalidResourceLabelException;

import java.util.Optional;
import java.util.regex.Pattern;

@Component
public final class ValidationHelperService {

    private static final Pattern ALNUM_START_END_PATTERN = Pattern.compile("^[a-zA-Z0-9](.*[a-zA-Z0-9])?$");
    private static final Pattern ALLOWED_CHARACTER_PATTERN = Pattern.compile("^[a-zA-Z0-9_.-]+$");
    private static final String DNS_LABEL_FORMAT = "[a-z0-9]([-a-z0-9]*[a-z0-9])?";
    private static final Pattern DNS_1123_SUBDOMAIN_PATTERN = Pattern
            .compile("^" + DNS_LABEL_FORMAT + "(\\." + DNS_LABEL_FORMAT + ")*$");

    private static final int MAX_KEY_NAME_LENGTH = 63;
    private static final int MAX_PREFIX_LENGTH = 253;

    static final int MAX_ANNOTATION_LENGTH = 1000;
    static final int MAX_LABEL_LENGTH = 63;

    private static final String ERROR_MESSAGE_TEMPLATE = "Error validating <%s:%s>; %s";
    private static final String EMPTY_KEY_ERROR = "Key cannot be empty.";
    private static final String MULTI_PREFIX_ERROR = "Key cannot have multiple prefixes.";
    private static final String EMPTY_KEY_PREFIX_ERROR = "Key prefix cannot be empty.";
    private static final String EMPTY_KEY_NAME_ERROR = "Key name cannot be empty.";
    private static final String LONG_KEY_NAME_ERROR = "Key name cannot be more than 63 characters.";
    private static final String START_END_KEY_NAME_ERROR = "Key name should start and end with [a-zA-Z0-9].";
    private static final String START_END_LABEL_ERROR = "Label should start and end with [a-zA-Z0-9].";
    private static final String KEY_NAME_CHARACTER_ERROR = "Key name should use only [a-zA-Z0-9] and `-`, '_', and `.`";
    private static final String LABEL_CHARACTER_ERROR = "Label should use only [a-zA-Z0-9] and `-`, '_', and `.`";
    private static final String LONGER_PREFIX_ERROR = "Key prefix cannot be more than 253 characters.";
    private static final String INVALID_PREFIX_ERROR = "Key prefix must follow RFC 1123 DNS subdomain format. \n" +
            "It must contain one or more segments separated by '.'(dot), and each segment must start and end with " +
            "[a-z0-9], and may contain - (hyphen) and [a-z0-9] in between.";
    private static final String PREFIX_SEPARATOR = "/";
    private static final String LONG_ANNOTATION_ERROR = "Annotation cannot be more than " + MAX_ANNOTATION_LENGTH
            + " characters long.";
    private static final String LONG_LABEL_ERROR = "LABEL cannot be more than " + MAX_LABEL_LENGTH + "charecters long.";

    public void checkAnnotations(final ResourceAnnotations annotations) throws InvalidResourceAnnotationException {
        if (annotations != null) {
            annotations.forEach(this::validateAnnotation);
        }
    }

    public void checkLabels(final ResourceLabels labels) throws InvalidResourceLabelException {
        if (labels != null) {
            labels.forEach(this::validateLabels);
        }
    }

    private void validateAnnotation(final String key, final String value) {
        validateKey(key).ifPresent(error -> {
            throw new InvalidResourceAnnotationException(String.format(ERROR_MESSAGE_TEMPLATE, key, value, error));
        });
        if (value != null && value.length() > MAX_ANNOTATION_LENGTH) {
            throw new InvalidResourceAnnotationException(
                    String.format(ERROR_MESSAGE_TEMPLATE, key, value, LONG_ANNOTATION_ERROR));
        }
    }

    private void validateLabels(final String key, final String value) {
        validateKey(key).ifPresent(error -> {
            throw new InvalidResourceLabelException(error);
        });
        if (value == null || value.isEmpty()) {
            return;
        }
        if (value.length() > MAX_LABEL_LENGTH) {
            throw new InvalidResourceLabelException(LONG_LABEL_ERROR);
        }
        if (!ALNUM_START_END_PATTERN.matcher(value).matches()) {
            throw new InvalidResourceLabelException(START_END_LABEL_ERROR);
        }
        if (!ALLOWED_CHARACTER_PATTERN.matcher(value).matches()) {
            throw new InvalidResourceLabelException(LABEL_CHARACTER_ERROR);
        }
    }

    /*
     * Both Annotation and Label key share the same format.
     */
    private Optional<String> validateKey(final String key) {

        if (key.isEmpty()) {
            return Optional.of(EMPTY_KEY_ERROR);
        }

        final String[] keyParts = key.split(PREFIX_SEPARATOR, -1);
        final String keyName;
        final Optional<String> keyPrefix;
        if (keyParts.length == 1) {
            keyPrefix = Optional.empty();
            keyName = key;
        } else if (keyParts.length == 2) {
            keyPrefix = Optional.of(keyParts[0]);
            keyName = keyParts[1];
        } else {
            return Optional.of(MULTI_PREFIX_ERROR);
        }

        final Optional<String> prefixError = validateKeyPrefix(keyPrefix);
        if (prefixError.isPresent()) {
            return prefixError;
        }
        return validateKeyName(keyName);
    }

    private Optional<String> validateKeyPrefix(final Optional<String> keyPrefix) {
        if (keyPrefix.isEmpty()) {
            return Optional.empty();
        }
        final String prefix = keyPrefix.get();
        if (prefix.isEmpty()) {
            return Optional.of(EMPTY_KEY_PREFIX_ERROR);
        }
        if (prefix.length() > MAX_PREFIX_LENGTH) {
            return Optional.of(LONGER_PREFIX_ERROR);
        }
        if (!DNS_1123_SUBDOMAIN_PATTERN.matcher(prefix).matches()) {
            return Optional.of(INVALID_PREFIX_ERROR);
        }
        return Optional.empty();
    }

    private Optional<String> validateKeyName(final String keyName) {
        if (keyName.isEmpty()) {
            return Optional.of(EMPTY_KEY_NAME_ERROR);
        }
        if (keyName.length() > MAX_KEY_NAME_LENGTH) {
            return Optional.of(LONG_KEY_NAME_ERROR);
        }
        if (!ALNUM_START_END_PATTERN.matcher(keyName).matches()) {
            return Optional.of(START_END_KEY_NAME_ERROR);
        }
        if (!ALLOWED_CHARACTER_PATTERN.matcher(keyName).matches()) {
            return Optional.of(KEY_NAME_CHARACTER_ERROR);
        }
        return Optional.empty();

    }
}
