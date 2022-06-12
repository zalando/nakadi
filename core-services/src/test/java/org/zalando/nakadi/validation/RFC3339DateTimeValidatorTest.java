package org.zalando.nakadi.validation;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.utils.IsOptional;

public class RFC3339DateTimeValidatorTest {
    private final RFC3339DateTimeValidator validator = new RFC3339DateTimeValidator();

    @Test
    public void requireMetadataOccurredAtToBeFormattedAsDateTime() {
        final String invalidDateTimes[] = new String[]{
                "x", // totally non sense string
                "1996-10-15T16:39:57+07:00:30", // invalid seconds in the offset
                "1996-10-15T16:39:57+0700", // invalid missing colon in the offset
                "1996-10-15T16:39:57 07:00", // invalid missing signal in the offset
                "1996-10-15T16:39:57.1234567890Z", // invalid 10 digits milliseconds
                "1996-10-45T16:39:57Z", // check for lenience (there are no months with 45 days)
                "1996-10-15 16:39:57Z", // requires "T" as separator
                "1996-10-15T16:39:57", // offsets are required
                "1996-10-15T16:39Z", // seconds are required
        };

        final String validDateTimes[] = new String[]{
                "1996-10-15T16:39:57+07:00", // just a very simple example
                "1996-10-15T16:39:57-07:00", // just a very simple example
                "1996-10-15T16:39:57.123+07:00", // simple example with milliseconds
                "1996-10-15T16:39:57.1234Z", // tricky 4 milliseconds digits (yes it's valid, sorry)
                "1996-10-15t16:39:57Z", // the RFC requires uppercase T or lowercase t
                "1996-10-15T16:39:57z", // the RFC requires uppercase Z or lowercase z
                "1996-10-15T16:39:57.123456789Z", // valid up to 9 milliseconds digits
                "1996-10-15T16:39:57.12Z", // yes, it' valid, just 2 milliseconds digits
        };

        for (final String invalid : invalidDateTimes) {
            Assert.assertThat(invalid, validator.validate(invalid), IsOptional.isPresent());
        }

        for (final String valid : validDateTimes) {
            Assert.assertThat(valid, validator.validate(valid), IsOptional.isAbsent());
        }
    }
}
