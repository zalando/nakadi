package de.zalando.aruha.nakadi.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

public class TestUtils {

    private static final Random RANDOM = new Random();

    private TestUtils() { }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static int randomUInt() {
        return RANDOM.nextInt(Integer.MAX_VALUE);
    }

    public static String randomUIntAsString() {
        return Integer.toString(randomUInt());
    }

    public static long randomULong() {
        return randomUInt() * randomUInt();
    }

    public static String randomULongAsString() {
        return Long.toString(randomULong());
    }

    public static String resourceAsString(final String resourceName, final Class clazz) throws IOException {
        return IOUtils.toString(clazz.getResourceAsStream(resourceName));
    }

}
