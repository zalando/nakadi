package org.zalando.nakadi.domain;

import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BatchFactory {

    private static int navigateToObjectStart(final int from, final int end, final String data) {
        int curPos = from;
        char currentChar;
        while (curPos < end && (currentChar = data.charAt(curPos)) != '{') {
            if (currentChar != ',' && !isEmptyCharacter(currentChar)) {
                throw new JSONException("Illegal character at position " + curPos);
            }
            ++curPos;
        }
        final boolean found = curPos != end;
        return found ? curPos : -1;
    }

    private static int navigateToObjectEnd(final int from, final int end, final String data,
                                           final Consumer<BatchItem> batchItemConsumer) {
        int curPos = from;
        int nestingLevel = 0;
        boolean escaped = false;
        boolean insideQuote = false;
        boolean hasFields = false;
        int injectionPointStart = -1;

        final BatchItem.InjectionConfiguration[] injections =
                new BatchItem.InjectionConfiguration[BatchItem.Injection.values().length];
        final List<Integer> skipPositions = new ArrayList<>();

        while (curPos < end) {
            final char curChar = data.charAt(curPos);
            if (!insideQuote && shouldBeSkipped(curChar)) {
                skipPositions.add(curPos - from);
            }
            if (!escaped && curChar == '"') {
                insideQuote = !insideQuote;
                if (insideQuote && nestingLevel == 1 && injectionPointStart == -1) {
                    injectionPointStart = curPos;
                    hasFields = true;
                }
            }
            if (escaped) {
                escaped = false;
            } else if (!escaped && curChar == '\\') {
                escaped = true;
            } else if (!insideQuote) {
                BatchItem.InjectionConfiguration toAdd = null;
                if (curChar == '{') {
                    ++nestingLevel;
                } else if (curChar == '}') {
                    --nestingLevel;
                    if (nestingLevel == 1 && injectionPointStart != -1) {
                        toAdd = extractInjection(from, injectionPointStart, curPos + 1, data);
                        injectionPointStart = -1;
                    }
                    if (nestingLevel == 0) {
                        break;
                    }
                } else if (nestingLevel == 1 && curChar == ',' && injectionPointStart != -1) {
                    toAdd = extractInjection(from, injectionPointStart, curPos, data);
                    injectionPointStart = -1;
                }
                if (null != toAdd) {
                    injections[toAdd.injection.ordinal()] = toAdd;
                }
            }
            ++curPos;
        }
        if (curPos == data.length()) {
            return -1;
        }
        batchItemConsumer.accept(
                new BatchItem(
                        data.substring(from, curPos + 1),
                        BatchItem.EmptyInjectionConfiguration.build(1, hasFields),
                        injections,
                        skipPositions));
        return curPos;
    }

    private static BatchItem.InjectionConfiguration extractInjection(
            final int messageOffset,
            final int injectionPointStart,
            final int end,
            final String data) {
        for (final BatchItem.Injection type : BatchItem.Injection.values()) {
            if ((end - injectionPointStart - 3) < type.name.length()) {
                continue;
            }
            boolean matches = data.charAt(injectionPointStart + 1 + type.name.length()) == '"';
            if (matches) {
                for (int i = 0; i < type.name.length(); ++i) {
                    if (data.charAt(injectionPointStart + i + 1) != type.name.charAt(i)) {
                        matches = false;
                        break;
                    }
                }
            }
            if (matches) {
                return new BatchItem.InjectionConfiguration(
                        type, injectionPointStart - messageOffset, end - messageOffset);
            }
        }
        return null;
    }

    public static List<BatchItem> from(final String events) {
        final List<BatchItem> batch = new ArrayList<>();
        int objectStart = locateOpenSquareBracket(events) + 1;
        final int arrayEnd = locateClosingSquareBracket(objectStart, events);

        while (-1 != (objectStart = navigateToObjectStart(objectStart, arrayEnd, events))) {
            final int objectEnd = navigateToObjectEnd(objectStart, arrayEnd, events, batch::add);
            if (objectEnd == -1) {
                throw new JSONException("Unclosed object staring at " + objectStart + " found.");
            }
            objectStart = objectEnd + 1;
        }

        return batch;
    }

    private static int locateOpenSquareBracket(final String events) {
        int pos = 0;
        while (pos < events.length() && isEmptyCharacter(events.charAt(pos))) {
            ++pos;
        }
        if (events.charAt(pos) != '[') {
            throw new JSONException("Array of events should start with [ at position " + pos);
        }
        return pos;
    }

    private static int locateClosingSquareBracket(final int start, final String events) {
        int pos = events.length() - 1;
        while (pos >= start && isEmptyCharacter(events.charAt(pos))) {
            --pos;
        }
        if (events.charAt(pos) != ']') {
            throw new JSONException("Array of events should end with ] at position " + pos);
        }
        return pos;
    }

    static boolean shouldBeSkipped(final char c) {
        return (c == '\r' || c == '\n' || c == ' ' || c == '\t');
    }

    static boolean isEmptyCharacter(final char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
    }
}
