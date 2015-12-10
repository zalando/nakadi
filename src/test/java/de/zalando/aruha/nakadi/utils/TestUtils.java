package de.zalando.aruha.nakadi.utils;

import java.util.Random;
import java.util.UUID;

public class TestUtils {

    private static final Random random = new Random();

    private TestUtils() {
    }

    public static String randomString() {
        return UUID.randomUUID().toString();
    }

    public static int randomUInt() {
        return Math.abs(random.nextInt());
    }
}
