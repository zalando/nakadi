package de.zalando.aruha.nakadi.webservice.hila;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.domain.Cursor;

import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;

@Immutable
public class StreamBatch {

    private Cursor cursor;
    private List<Map> events;

    public StreamBatch(@JsonProperty("cursor") final Cursor cursor, @JsonProperty("events") final List<Map> events) {
        this.cursor = cursor;
        this.events = Optional.ofNullable(events).orElse(ImmutableList.of());
    }

    public Cursor getCursor() {
        return cursor;
    }

    public List<Map> getEvents() {
        return unmodifiableList(events);
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final Map event) {
        return new StreamBatch(new Cursor(partition, offset), ImmutableList.of(event));
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final StreamBatch that = (StreamBatch) o;
        return cursor.equals(that.cursor) && events.equals(that.events);
    }

    @Override
    public int hashCode() {
        return 31 * cursor.hashCode() + events.hashCode();
    }
}
