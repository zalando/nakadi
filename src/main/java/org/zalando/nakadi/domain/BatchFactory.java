package org.zalando.nakadi.domain;

import org.json.JSONArray;

import java.util.ArrayList;
import java.util.List;

public class BatchFactory {

    public static List<BatchItem> from(final JSONArray events) {
        final List<BatchItem> batch = new ArrayList<>(events.length());
        for (int i = 0; i < events.length(); i++) {
            batch.add(new BatchItem(events.getJSONObject(i)));
        }

        return batch;
    }

    public static List<BatchItem> from(final String events) {
        final List<BatchItem> batch = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        int brackets = 0;
        for (int i = 1; i < events.length() - 1; i++) {
            if (events.charAt(i) == '{') {
                brackets++;
            }
            if (events.charAt(i) == '}') {
                brackets--;
            }
            if (!((brackets == 0) && (events.charAt(i) == ','))) {
                sb.append(events.charAt(i));
            }
            if (brackets == 0) {
                if (sb.length() > 0) {
                    batch.add(new BatchItem(sb.toString()));
                }
                sb = new StringBuilder();
            }
        }

        if (sb.length() != 0) {
            batch.add(new BatchItem(sb.toString()));
        }

        return batch;
    }

}
