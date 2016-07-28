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

}
