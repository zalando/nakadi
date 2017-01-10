package org.zalando.nakadi.domain;

import org.json.JSONException;

import java.util.ArrayList;
import java.util.List;

public class BatchFactory {

    public static List<BatchItem> from(final String events) {
        final List<BatchItem> batch = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int brackets = 0;
        boolean insideQuote = false;
        boolean escaped = false;
        int start = 0;
        final int length = events.length();
        int end = length - 1;

        while ((events.charAt(start) == ' '
                || events.charAt(start) == '\t'
                || events.charAt(start) == '\n'
                || events.charAt(start) == '\r')
                && start < end) {
            start++;
        }
        while ((events.charAt(end) == ' '
                || events.charAt(end) == '\t'
                || events.charAt(end) == '\n'
                || events.charAt(end) == '\r')
                && end > start) {
            end--;
        }
        if (!(events.charAt(start) == '[')) {
            throw new JSONException(String.format("Unexpected character %s in position %d, expected '['",
                    events.charAt(start), start));
        }
        start++;
        if (!(events.charAt(end) == ']')) {
            throw new JSONException(String.format("Unexpected character %s in position %d, expected ']'",
                    events.charAt(end), end));
        }

        for (int i = start; i < end; i++) {
            if (!escaped && events.charAt(i) == '"') {
                if (insideQuote) {
                    insideQuote = false;
                } else {
                    insideQuote = true;
                }
            }
            if (escaped) {
                sb.append(events.charAt(i));
                escaped = false;
            } else if (!escaped && events.charAt(i) == '\\') {
                sb.append(events.charAt(i));
                escaped = true;
            } else if (insideQuote) {
                sb.append(events.charAt(i));
            } else {
                if (events.charAt(i) == '{') {
                    brackets++;
                }
                if (events.charAt(i) == '}') {
                    brackets--;
                }
                if (!((brackets == 0) && ((events.charAt(i) == ',')
                 || (events.charAt(i) == ' ')
                || (events.charAt(i) == '\t')
                || (events.charAt(i) == '\n')
                || (events.charAt(i) == '\r')))) {
                    sb.append(events.charAt(i));
                }
                if (brackets == 0 && (events.charAt(i) != ' ')
                        && (events.charAt(i) != '\t'
                        && (events.charAt(i) != '\n')
                        && (events.charAt(i) != '\r'))) {
                    if (sb.length() > 0) {
                        batch.add(new BatchItem(sb.toString()));
                    }
                    sb = new StringBuilder();
                }
            }
        }

        if (sb.length() != 0) {
            batch.add(new BatchItem(sb.toString()));
        }

        return batch;
    }
}
