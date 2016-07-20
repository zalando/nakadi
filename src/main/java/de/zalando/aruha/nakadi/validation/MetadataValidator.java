package de.zalando.aruha.nakadi.validation;

import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;
import org.json.JSONObject;

import java.util.Optional;

public class MetadataValidator implements EventValidator {
    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        return Optional
                .ofNullable(event.optJSONObject("metadata"))
                .map(metadata -> metadata.optString("occurred_at"))
                .flatMap(this::checkDateTime);
    }

    private Optional<ValidationError> checkDateTime(final String occurredAt) {
        try {
            final DateTimeFormatter dateFormatter = ISODateTimeFormat.dateTimeParser();
            dateFormatter.parseDateTime(occurredAt);

            return Optional.empty();
        } catch (IllegalArgumentException e) {
            return Optional.of(new ValidationError("occurred_at must be a valid date-time"));
        }
    }
}
