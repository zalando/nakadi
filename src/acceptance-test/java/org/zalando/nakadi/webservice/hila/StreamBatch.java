package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.json.JSONObject;
import org.zalando.nakadi.domain.SubscriptionCursor;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Collections.unmodifiableList;

@Immutable
public class StreamBatch {

    private static final String DUMMY_TOKEN = "dummy-token";

    private final SubscriptionCursor cursor;
    private final List<Map> events;
    private final JSONObject metadata;

    public StreamBatch(@JsonProperty("cursor") final SubscriptionCursor cursor,
                       @Nullable @JsonProperty("events") final List<Map> events,
                       @Nullable @JsonProperty("metadata") final JSONObject metadata) {
        this.cursor = cursor;
        this.events = Optional.ofNullable(events).orElse(ImmutableList.of());
        this.metadata = metadata;
    }

    public SubscriptionCursor getCursor() {
        return cursor;
    }

    public List<Map> getEvents() {
        return unmodifiableList(events);
    }

    public JSONObject getMetadata() {
        return metadata;
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final Map event, String metadata) {
        return new StreamBatch(new SubscriptionCursor(partition, offset, eventType, DUMMY_TOKEN),
                ImmutableList.of(event), new JSONObject("{\"debug\":\""+metadata+"\"}"));
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final Map event) {
        return new StreamBatch(new SubscriptionCursor(partition, offset, eventType, DUMMY_TOKEN),
                ImmutableList.of(event), null);
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

    @Override
    public String toString() {
        return "StreamBatch{" +
                "cursor=" + cursor +
                ", events=" + events +
                '}';
    }

    public static class MatcherIgnoringToken extends BaseMatcher<StreamBatch> {

        private final StreamBatch batch;

        public MatcherIgnoringToken(final StreamBatch batch) {
            this.batch = batch;
        }

        @Override
        public boolean matches(final Object item) {
            if (!(item instanceof StreamBatch)) {
                return false;
            }
            final StreamBatch batchTocheck = (StreamBatch) item;
            final SubscriptionCursor cursor = batch.getCursor();
            final SubscriptionCursor cursorToCheck = batchTocheck.getCursor();

            return batch.getEvents().equals(batchTocheck.getEvents()) &&
                    cursor.getPartition().equals(cursorToCheck.getPartition()) &&
                    cursor.getOffset().equals(cursorToCheck.getOffset()) &&
                    cursor.getEventType().equals(cursorToCheck.getEventType()) &&
                    batch.getMetadata().equals(batchTocheck.getMetadata());
        }

        @Override
        public void describeTo(final Description description) {
        }

        public static MatcherIgnoringToken equalToBatchIgnoringToken(final StreamBatch batch) {
            return new MatcherIgnoringToken(batch);
        }
    }
}
