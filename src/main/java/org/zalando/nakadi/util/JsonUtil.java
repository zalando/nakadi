package org.zalando.nakadi.util;

public class JsonUtil {

    /**
     * bugifx ARUHA-563
     *
     * @param eventTypeSchema
     */
    public static boolean isValidJson(final String eventTypeSchema) {
        final char[] chars = eventTypeSchema.toCharArray();
        int bracketsCounter = 0;
        for (int i = 0; i < chars.length; i++) {
            if (chars[i] == '{' || chars[i] == '[') {
                bracketsCounter++;
            } else if (chars[i] == '}' || chars[i] == ']') {
                if (chars[i - 1] == ',') {
                    return false;
                }
                bracketsCounter--;
            }
            // if all brackets were closed but we still have symbols in schema then something wrong with the schema
            if (bracketsCounter == 0 && i + 1 < chars.length) {
                return false;
            }
        }
        return true;
    }
}
