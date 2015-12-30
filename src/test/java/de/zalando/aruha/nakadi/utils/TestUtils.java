package de.zalando.aruha.nakadi.utils;

import java.util.Random;
import java.util.UUID;

public class TestUtils {

    private static final Random random = new Random();

    private TestUtils() { }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static int randomUInt() {
        return Math.abs(random.nextInt());
    }

    public static String randomUIntAsString() {
        return Integer.toString(randomUInt());
    }

    public static long randomULong() {
        return Math.abs(random.nextLong());
    }

    public static String randomULongAsString() {
        return Long.toString(randomULong());
    }
}
