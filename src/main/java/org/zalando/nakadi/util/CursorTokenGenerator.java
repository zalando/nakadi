package org.zalando.nakadi.util;

import java.util.UUID;


public class CursorTokenGenerator {

    public static String generateCursorToken() {
        return UUID.randomUUID().toString();
    }
}
