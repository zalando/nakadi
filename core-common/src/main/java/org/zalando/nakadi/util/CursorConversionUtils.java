package org.zalando.nakadi.util;

import java.util.regex.Pattern;

public class CursorConversionUtils {
    public static final Pattern NUMBERS_ONLY_PATTERN = Pattern.compile("^-?[0-9]+$");
}
