package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.time.format.DateTimeParseException;
import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RFC3339DateTimeValidator implements FormatValidator {

    private static final String DATE_TIME_OFFSET_PATTERN =
            "^\\d{4}-([0][1-9]|[1][0-2])-[0-3]\\d{1}[Tt][0-2]\\d{1}:[0-5]\\d{1}:[0-5]\\d{1}.?\\d{0,9}([zZ]|([+-]\\d{2}:\\d{2}))$";
    private static final Pattern PATTERN = Pattern.compile(DATE_TIME_OFFSET_PATTERN);
    private static final Optional<String> MUST_BE_A_VALID_DATE_TIME = Optional.of("must be a valid date-time");

    @Override
    public Optional<String> validate(final String dateTime) {
        try {
            final Matcher matcher = PATTERN.matcher(dateTime);
            if (matcher.matches()) {
                return Optional.empty();
            } else {
                return MUST_BE_A_VALID_DATE_TIME;
            }

        } catch (final DateTimeParseException e) {
            return MUST_BE_A_VALID_DATE_TIME;
        }
    }
}
