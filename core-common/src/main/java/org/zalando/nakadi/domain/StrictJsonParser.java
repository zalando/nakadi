package org.zalando.nakadi.domain;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StrictJsonParser {

    private static final Logger LOG = LoggerFactory.getLogger(StrictJsonParser.class);

    private static final String POSSIBLE_NUMBER_DIGITS = "0123456789-+.Ee";

    private static class StringTokenizer {

        private int currentPosition;
        private final String value;
        private final int endIndex;

        StringTokenizer(final String value, final int from, final int to) {
            this.value = value;
            this.currentPosition = from;
            this.endIndex = to;
        }

        boolean hasNext() {
            return currentPosition < endIndex;
        }

        char next() {
            if (currentPosition >= endIndex) {
                throw new JSONException("Unexpected end of data at pos " + currentPosition);
            }
            return value.charAt(currentPosition++);
        }

        int getCurrentPosition() {
            return currentPosition;
        }

        String next(final int count) {
            if (currentPosition + count >= endIndex) {
                throw new JSONException("Unexpected end of data at pos " + currentPosition);
            }
            currentPosition += count;
            return value.substring(currentPosition - count, currentPosition);
        }

        char nextUnskippable() {
            for (; ; ) {
                final char value = next();
                if (isEmptyCharacter(value)) {
                    continue;
                }
                return value;
            }
        }

        void back() {
            this.currentPosition--;
        }

        static boolean isEmptyCharacter(final char c) {
            return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
        }
    }

    public static JSONObject parseObject(final String value) throws JSONException {
        return parse(value, true);
    }

    public static JSONObject parse(final String value, final boolean allowMore) throws JSONException {
        try {
            return (JSONObject) parse(value, 0, value.length(), allowMore);
        } catch (final JSONException e) {
            // temporary logging
            LOG.debug("[STRICT_JSON_FAIL] Failed to parse json with strict parser: {} Error message: {}",
                    value, e.getMessage());
            throw e;
        }
    }

    private static Object parse(final String value, final int startIdx, final int endIdx, final boolean allowMore)
            throws JSONException {
        final StringTokenizer stringTokenizer = new StringTokenizer(value, startIdx, endIdx);
        final Object result = parse(stringTokenizer);
        if (!allowMore) {
            final char unexpectedValue;
            try {
                unexpectedValue = stringTokenizer.nextUnskippable();
            } catch (JSONException ignore) {
                return result;
            }
            throw syntaxError("Unexpected symbol '" + unexpectedValue + "'", stringTokenizer);
        } else {
            return result;
        }
    }

    private static Object parse(final StringTokenizer tokenizer) {
        final char value = tokenizer.nextUnskippable();
        switch (value) {
            case '{':
                return readObjectTillTheEnd(tokenizer);
            case '[':
                return readArrayTillTheEnd(tokenizer);
            case '"':
                return readStringTillTheEnd(tokenizer);
            case 'n':
                return readNullTillTheEnd(tokenizer);
            case 't':
                return readTrueTillTheEnd(tokenizer);
            case 'f':
                return readFalseTillTheEnd(tokenizer);
            case '0':
            case '1':
            case '2':
            case '3':
            case '4':
            case '5':
            case '6':
            case '7':
            case '8':
            case '9':
            case '-':
                return readNumberTillTheEnd(value, tokenizer);
            default:
                throw syntaxError("Unexpected symbol '" + value + "'", tokenizer);
        }
    }

    private static Object readFalseTillTheEnd(final StringTokenizer tokenizer) {
        if (tokenizer.next(4).equals("alse")) {
            return Boolean.FALSE;
        } else {
            throw syntaxError("Expected false value", tokenizer);
        }
    }

    private static Object readTrueTillTheEnd(final StringTokenizer tokenizer) {
        if (tokenizer.next(3).equals("rue")) {
            return Boolean.TRUE;
        } else {
            throw syntaxError("Expected true value", tokenizer);
        }
    }

    private static Object readNumberTillTheEnd(final char value, final StringTokenizer tokenizer) {
        final int start = tokenizer.getCurrentPosition() - 1;
        while (tokenizer.hasNext()) {
            if (POSSIBLE_NUMBER_DIGITS.indexOf(tokenizer.next()) < 0) {
                tokenizer.back();
                break;
            }
        }
        final int finish = tokenizer.getCurrentPosition();
        final String stringNumber = tokenizer.value.substring(start, finish);

        if (stringNumber.indexOf('.') > -1 || stringNumber.indexOf('e') > -1
                || stringNumber.indexOf('E') > -1 || "-0".equals(stringNumber)) {
            final Double d = Double.valueOf(stringNumber);
            if (!d.isInfinite() && !d.isNaN()) {
                return d;
            } else {
                throw syntaxError(stringNumber + " can not be used", tokenizer);
            }
        } else {
            try {
                final long longValue = Long.parseLong(stringNumber);
                if (longValue <= Integer.MAX_VALUE && longValue >= Integer.MIN_VALUE) {
                    return (int) longValue;
                }
                return longValue;
            } catch (NumberFormatException e) {
                throw syntaxError("Can not use long value '" + stringNumber + "' cause it is too big", tokenizer);
            }
        }
    }

    private static Object readObjectTillTheEnd(final StringTokenizer tokenizer) {
        final JSONObject result = new JSONObject();
        boolean finished = false;
        boolean allowObjectEnd = true;
        while (!finished) {
            final char nameStart = tokenizer.nextUnskippable();
            if (nameStart == '}') {
                if (!allowObjectEnd) {
                    throw syntaxError("Not allowed to finish object with comma", tokenizer);
                }
                finished = true;
            } else {
                if (nameStart != '"') {
                    throw syntaxError("Unexpected symbol '" + nameStart + "'", tokenizer);
                }
                final String name = readStringTillTheEnd(tokenizer);
                final char separator = tokenizer.nextUnskippable();
                if (separator != ':') {
                    throw syntaxError("Waiting for name-value separator : while parsing object", tokenizer);
                }
                final Object value = parse(tokenizer);
                result.putOnce(name, value);
                final char nextToken = tokenizer.nextUnskippable();
                if (nextToken == '}') {
                    finished = true;
                } else if (nextToken != ',') {
                    throw syntaxError("Unexpected symbol '" + nextToken + "' while parsing object", tokenizer);
                }
            }
            allowObjectEnd = false;
        }
        return result;
    }

    private static Object readNullTillTheEnd(final StringTokenizer tokenizer) {
        if (!tokenizer.next(3).equals("ull")) {
            throw syntaxError("Expected null value", tokenizer);
        }
        return JSONObject.NULL;
    }

    private static Object readArrayTillTheEnd(final StringTokenizer tokenizer) {
        // 1. Check if it is empty array.
        final JSONArray result = new JSONArray();
        final char possibleEnd = tokenizer.nextUnskippable();
        if (possibleEnd == ']') {
            return result;
        }
        tokenizer.back();
        boolean finished = false;
        while (!finished) {
            result.put(parse(tokenizer));
            final char separator = tokenizer.nextUnskippable();
            if (separator == ']') {
                finished = true;
            } else if (separator != ',') {
                throw syntaxError("Unexpected separator '" + separator + "'", tokenizer);
            }
        }
        return result;
    }

    private static JSONException syntaxError(final String message, final StringTokenizer tokenizer) {
        return new JSONException(message + " at pos " + tokenizer.currentPosition);
    }

    private static String readStringTillTheEnd(final StringTokenizer tokenizer) {
        final StringBuilder sb = new StringBuilder();
        boolean finished = false;
        while (!finished) {
            char c = tokenizer.next();
            if (isControlCharacter(c)) {
                throw syntaxError("Illegal escape.", tokenizer);
            }
            switch (c) {
                case 0:
                case '\n':
                case '\r':
                    throw syntaxError("Unterminated string", tokenizer);
                case '\\':
                    c = tokenizer.next();
                    switch (c) {
                        case 'b':
                            sb.append('\b');
                            break;
                        case 't':
                            sb.append('\t');
                            break;
                        case 'n':
                            sb.append('\n');
                            break;
                        case 'f':
                            sb.append('\f');
                            break;
                        case 'r':
                            sb.append('\r');
                            break;
                        case 'u':
                            final String codepoint = tokenizer.next(4);
                            try {
                                sb.append((char) Integer.parseInt(codepoint, 16));
                            } catch (NumberFormatException e) {
                                throw syntaxError("Illegal codepoint " + codepoint, tokenizer);
                            }
                            break;
                        case '"':
                        case '\'':
                        case '\\':
                        case '/':
                            sb.append(c);
                            break;
                        default:
                            throw syntaxError("Illegal escape.", tokenizer);
                    }
                    break;
                case '"':
                    finished = true;
                    break;
                default:
                    sb.append(c); // TODO: Possible optimization
            }
        }
        return sb.toString();
    }

    private static boolean isControlCharacter(final char c) {
        return c >= '\u0000' && c <= '\u001F';
    }

}
