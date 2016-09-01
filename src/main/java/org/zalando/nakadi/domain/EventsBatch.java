package org.zalando.nakadi.domain;

import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;

@Immutable
public class EventsBatch {

    private final Cursor cursor;

    private final List<String> events;

    public EventsBatch(final Cursor cursor, final List<String> events) {
        this.cursor = cursor;
        this.events = events;
    }

    public Cursor getCursor() {
        return cursor;
    }

    public List<String> getEvents() {
        return Collections.unmodifiableList(events);
    }
}
