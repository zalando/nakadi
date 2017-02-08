package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.zalando.nakadi.domain.StreamMetadata;
import org.zalando.nakadi.view.SubscriptionCursor;

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
    private final StreamMetadata metadata;

    public StreamBatch(@JsonProperty("cursor") final SubscriptionCursor cursor,
                       @Nullable @JsonProperty("events") final List<Map> events,
                       @Nullable @JsonProperty("info") final StreamMetadata metadata) {
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

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final StreamBatch that = (StreamBatch) o;
        return cursor.equals(that.cursor) && events.equals(that.events)
                && (metadata != null ? metadata.equals(that.metadata) : that.metadata == null);
    }

    @Override
    public int hashCode() {
        int result = cursor.hashCode();
        result = 31 * result + events.hashCode();
        result = 31 * result + (metadata != null ? metadata.hashCode() : 0);
        return result;
    }

    @Nullable
    public StreamMetadata getMetadata() {
        return metadata;
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final Map event, final String metadata) {
        if (event.isEmpty()) {
            return new StreamBatch(new SubscriptionCursor(partition, offset, eventType, DUMMY_TOKEN),
                    ImmutableList.of(), new StreamMetadata(metadata));
        } else {
            return new StreamBatch(new SubscriptionCursor(partition, offset, eventType, DUMMY_TOKEN),
                    ImmutableList.of(event), new StreamMetadata(metadata));
        }
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final Map event) {
        return new StreamBatch(new SubscriptionCursor(partition, offset, eventType, DUMMY_TOKEN),
                ImmutableList.of(event), null);
    }

    @Override
    public String toString() {
        return "StreamBatch{" +
                "cursor=" + cursor +
                ", events=" + events +
                ", metadata=" + metadata +
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
                    Optional.ofNullable(batch.getMetadata())
                            .map(b -> b.equals(batchTocheck.getMetadata()))
                            .orElse(batchTocheck.getMetadata() == null);
        }

        @Override
        public void describeTo(final Description description) {
        }

        public static MatcherIgnoringToken equalToBatchIgnoringToken(final StreamBatch batch) {
            return new MatcherIgnoringToken(batch);
        }
    }
}
