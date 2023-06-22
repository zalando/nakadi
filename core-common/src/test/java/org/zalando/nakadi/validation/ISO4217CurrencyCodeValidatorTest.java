package org.zalando.nakadi.validation;

import org.junit.Test;
import org.zalando.nakadi.utils.IsOptional;

import static org.hamcrest.MatcherAssert.assertThat;

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

        final var validator = new ISO4217CurrencyCodeValidator("an-event-type");
        for (final String value : invalidValues) {
            // For invalid values, the validator should return empty result.
            assertThat("Test: " + value, validator.validate(value), IsOptional.isAbsent());
        }

        for (final String value : validValues) {
            assertThat("Test: " + value, validator.validate(value), IsOptional.isAbsent());
        }
    }
}
