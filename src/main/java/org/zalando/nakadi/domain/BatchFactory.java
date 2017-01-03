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
        if ((!events.startsWith("[")) || (!events.endsWith("]"))) {
            throw new JSONException("Array must be surrounded with square brackets");
        }
        for (int i = 1; i < events.length() - 1; i++) {
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
                if (!((brackets == 0) && (events.charAt(i) == ','))) {
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
