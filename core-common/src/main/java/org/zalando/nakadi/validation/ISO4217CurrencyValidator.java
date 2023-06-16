package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Currency;
import java.util.Optional;

public class ISO4217CurrencyValidator implements FormatValidator {
    private static final Logger LOG = LoggerFactory.getLogger(ISO4217CurrencyValidator.class);
    private final String eventTypeName;
    private final String formatName;

    ISO4217CurrencyValidator(final String eventTypeName) {
        this(eventTypeName, "iso-4217");
    }

    ISO4217CurrencyValidator(final String eventTypeName, final String formatName) {
        this.eventTypeName = eventTypeName;
        this.formatName = formatName;
    }

    @Override
    public Optional<String> validate(final String value) {
        try {
            Currency.getInstance(value);
        } catch (IllegalArgumentException ex) {
            // Note: not returning this as validation error (yet), but just log it.
            LOG.warn("Currency format violation (format_name={}): [event_type={}, value={}]",
                    formatName, eventTypeName, value);
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return formatName;
    }
}
