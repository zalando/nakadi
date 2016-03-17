package de.zalando.aruha.nakadi.validation;

import org.json.JSONObject;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Optional;

public class MetadataValidator implements EventValidator {
    private static final String DATETIME_FORMAT_STRING = "yyyy-MM-dd'T'HH:mm:ssXXX";
    private static final String DATETIME_FORMAT_STRING_SECFRAC = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";

    @Override
    public Optional<ValidationError> accepts(final JSONObject event) {
        return Optional
                .ofNullable(event.optJSONObject("metadata"))
                .map(metadata -> metadata.optString("occurred_at"))
                .flatMap(occurredAt -> checkDateTime(occurredAt));
    }

    private Optional<ValidationError> checkDateTime(final String occurredAt) {
        try {
            final DateFormat format = new SimpleDateFormat(DATETIME_FORMAT_STRING);
            format.setLenient(false);
            format.parse(occurredAt);

            return Optional.empty();
        } catch (ParseException e) {
            try {
                final DateFormat formatWithSecfrac = new SimpleDateFormat(DATETIME_FORMAT_STRING_SECFRAC);
                formatWithSecfrac.setLenient(false);
                Date date = formatWithSecfrac.parse(occurredAt);

                return Optional.empty();
            } catch (ParseException e1) {
                return Optional.of(new ValidationError("occurred_at must be a valid date-time"));
            }
        }
    }
}
