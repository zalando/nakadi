package org.zalando.nakadi.validation;

import org.everit.json.schema.FormatValidator;

import java.util.Currency;
import java.util.Optional;

public class ISO4217CurrencyCodeValidator implements FormatValidator {
    private final String formatName;

    ISO4217CurrencyCodeValidator() {
        this("iso-4217");
    }

    ISO4217CurrencyCodeValidator(final String formatName) {
        this.formatName = formatName;
    }

    @Override
    public Optional<String> validate(final String value) {
        try {
            Currency.getInstance(value);
        } catch (IllegalArgumentException ex) {
            return Optional.of(String.format("[%s] is not a valid alphabetic ISO 4217 currency code", value));
        }
        return Optional.empty();
    }

    @Override
    public String formatName() {
        return formatName;
    }
}
