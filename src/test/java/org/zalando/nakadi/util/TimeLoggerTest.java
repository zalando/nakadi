package org.zalando.nakadi.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.regex.Pattern;

public class TimeLoggerTest {

    @Test
    public void testEnclosedLogging() {
        final String smallLine;
        final String longLine;
        TimeLogger.startMeasure("Outer", "doStuff1");
        try {
            // do stuff 1
            TimeLogger.addMeasure("stuff2");
            // do stuff 2
            TimeLogger.startMeasure("Inner", "stuff2_1");
            try {
                // do stuff 2.1
            } finally {
                smallLine =TimeLogger.finishMeasure();
            }
            TimeLogger.addMeasure("stuff3");
            // do stuff 3
        } finally {
            longLine = TimeLogger.finishMeasure();
        }

        Assert.assertTrue(smallLine, Pattern.matches("^\\[Inner] \\[stuff2_1=\\d+]$", smallLine));
        Assert.assertTrue(longLine, Pattern.matches(
                "^\\[Outer] \\[doStuff1=\\d+] \\[stuff2=\\d+] \\[Inner=\\d+] \\[stuff3=\\d+]$", longLine));
    }

    @Test
    public void testLoggingWithoutStarting() {
        TimeLogger.addMeasure("Test");
        Assert.assertNull(TimeLogger.finishMeasure());
    }

}