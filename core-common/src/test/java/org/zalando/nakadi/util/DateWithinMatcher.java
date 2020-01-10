package org.zalando.nakadi.util;

import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.joda.time.DateTime;

import java.text.MessageFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class DateWithinMatcher extends BaseMatcher<String> {

    private final DateTime compareDate;
    private final ChronoUnit unit;
    private final long period;

    public DateWithinMatcher(final long period, final ChronoUnit unit, final DateTime compareDate) {
        this.compareDate = compareDate;
        this.unit = unit;
        this.period = period;
    }

    @Override
    public boolean matches(final Object item) {
        final DateTime date = DateTime.parse((String) item);
        final long datesDiff = Math.abs(date.getMillis() - compareDate.getMillis());
        return datesDiff <= Duration.of(period, unit).toMillis();
    }

    @Override
    public void describeTo(final Description description) {
        description.appendText(MessageFormat.format("date is within {0} {1} to date: {2}",
                period, unit.name(), compareDate.toString()));
    }

    public static DateWithinMatcher dateWithin(final long period, final ChronoUnit unit, final DateTime compareDate) {
        return new DateWithinMatcher(period, unit, compareDate);
    }
}