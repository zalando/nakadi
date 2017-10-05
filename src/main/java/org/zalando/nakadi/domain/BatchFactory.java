package org.zalando.nakadi.domain;

import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class BatchFactory {

    private static int navigateToObjectStart(final boolean isFirst, final int from, final int end, final String data) {
        int curPos = from;
        boolean commaPresent = false;
        char currentChar;
        while (curPos < end && (currentChar = data.charAt(curPos)) != '{') {
            if (currentChar == ',') {
                if (isFirst) {
                    throw new JSONException("Comma is not allowed at position " + curPos);
                } else {
                    commaPresent = true;
                }
            } else if (!isEmptyCharacter(currentChar)) {
                throw new JSONException("Illegal character at position " + curPos);
            }
            ++curPos;
        }
        final boolean found = curPos != end;
        if (found && !isFirst && !commaPresent) {
            throw new JSONException("Comma is not found at position " + curPos);
        }
        return found ? curPos : -1;
    }

    private static int navigateToObjectEnd(
            final int from, final int end, final String data, final Consumer<BatchItem> batchItemConsumer) {
        int curPos = from;
        int nestingLevel = 0;
        boolean escaped = false;
        boolean insideQuote = false;
        while (curPos < end) {
            final char curChar = data.charAt(curPos);
            if (!escaped && curChar == '"') {
                insideQuote = !insideQuote;
            }
            if (escaped) {
                escaped = false;
            } else if (!escaped && curChar == '\\') {
                escaped = true;
            } else if (!insideQuote) {
                if (curChar == '{') {
                    ++nestingLevel;
                } else if (curChar == '}') {
                    --nestingLevel;
                    if (nestingLevel == 0) {
                        break;
                    }
                }
            }
            ++curPos;
        }
        if (curPos == data.length()) {
            return -1;
        }
        batchItemConsumer.accept(new BatchItem(data.substring(from, curPos + 1)));
        return curPos;
    }

    public static List<BatchItem> from(final String events) {
        final List<BatchItem> batch = new ArrayList<>();
        int objectStart = locateOpenSquareBracket(events) + 1;
        final int arrayEnd = locateClosingSquareBracket(objectStart, events);

        while (-1 != (objectStart = navigateToObjectStart(batch.isEmpty(), objectStart, arrayEnd, events))) {
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

    private static boolean isEmptyCharacter(final char c) {
        return (c == ' ' || c == '\t' || c == '\n' || c == '\r');
    }
}
