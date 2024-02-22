package org.zalando.nakadi.plugin.auth.utils;

import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class Fixture {
    public static String fixture(final String fileName) throws IOException {
        return IOUtils.toString(Fixture.class.getResourceAsStream(fileName), StandardCharsets.UTF_8);
    }
}
