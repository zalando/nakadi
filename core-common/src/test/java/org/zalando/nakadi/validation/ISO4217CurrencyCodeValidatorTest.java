package org.zalando.nakadi.validation;

import org.junit.Test;
import org.zalando.nakadi.utils.IsOptional;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;

public class ISO4217CurrencyCodeValidatorTest {

    @Test
    public void testValidator() {
        final String invalidValues[] = new String[]{
                "x",
                "eur",
                "euro",
        };

        final String validValues[] = new String[]{
                "ALL",
                "AMD",
                "CHF",
                "CZK",
                "DKK",
                "EUR",
                "GBP",
                "ISK",
                "NOK",
                "PLN",
                "RON",
                "RSD",
                "SEK",
        };

        final var validator = new ISO4217CurrencyCodeValidator();
        for (final String value : invalidValues) {
            assertThat(
                    "Test: " + value,
                    validator.validate(value),
                    IsOptional.matches(containsString(" is not a valid alphabetic ISO 4217 currency code"))
            );
        }

        for (final String value : validValues) {
            assertThat("Test: " + value, validator.validate(value), IsOptional.isAbsent());
        }
    }
}
