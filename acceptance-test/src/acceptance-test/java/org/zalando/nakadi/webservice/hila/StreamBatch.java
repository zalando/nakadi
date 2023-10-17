package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.json.JSONArray;
import org.json.JSONObject;
import org.zalando.nakadi.domain.StreamMetadata;
import org.zalando.nakadi.repository.kafka.KafkaTestHelper;
import org.zalando.nakadi.view.SubscriptionCursor;

import javax.annotation.Nullable;
import javax.annotation.concurrent.Immutable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Immutable
public class StreamBatch {

    private static final String DUMMY_TOKEN = "dummy-token";

    private final SubscriptionCursor cursor;
    private final List<JSONObject> events;
    private final StreamMetadata metadata;

    // used for reading from string with object mapper
    public StreamBatch(@JsonProperty("cursor") final SubscriptionCursor cursor,
                       @Nullable @JsonProperty("events") final List<Map<String, Object>> events,
                       @Nullable @JsonProperty("info") final StreamMetadata metadata) {
        this.cursor = cursor;
        this.events = Optional.ofNullable(events)
                .map(evs -> evs.stream().map(JSONObject::new).collect(Collectors.toUnmodifiableList()))
                .orElse(Collections.emptyList());
        this.metadata = metadata;
    }

    public StreamBatch(final SubscriptionCursor cursor, final JSONArray eventsArray, final StreamMetadata metadata) {
        this.cursor = cursor;
        this.events = IntStream.range(0, eventsArray.length())
                .mapToObj(i -> eventsArray.getJSONObject(i))
                .collect(Collectors.toUnmodifiableList());
        this.metadata = metadata;
    }

    public SubscriptionCursor getCursor() {
        return cursor;
    }

    public List<JSONObject> getEvents() {
        return events;
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

    public static StreamBatch emptyBatch(final String partition, final String offset, final String eventType,
                                         final String debugInfo) {

        final String paddedOffset = StringUtils.leftPad(offset, KafkaTestHelper.CURSOR_OFFSET_LENGTH, '0');
        return new StreamBatch(
                new SubscriptionCursor(partition, paddedOffset, eventType, DUMMY_TOKEN),
                new JSONArray(),
                new StreamMetadata(debugInfo));
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final JSONObject event) {

        final String paddedOffset = StringUtils.leftPad(offset, KafkaTestHelper.CURSOR_OFFSET_LENGTH, '0');
        return new StreamBatch(
                new SubscriptionCursor(partition, paddedOffset, eventType, DUMMY_TOKEN),
                new JSONArray().put(event),
                null);
    }

    public static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                               final JSONObject event, final String debugInfo) {

        final String paddedOffset = StringUtils.leftPad(offset, KafkaTestHelper.CURSOR_OFFSET_LENGTH, '0');
        return new StreamBatch(
                new SubscriptionCursor(partition, paddedOffset, eventType, DUMMY_TOKEN),
                new JSONArray().put(event),
                new StreamMetadata(debugInfo));
    }

    private static StreamBatch singleEventBatch(final String partition, final String offset, final String eventType,
                                                final JSONObject event, final StreamMetadata metadata) {

        final String paddedOffset = StringUtils.leftPad(offset, KafkaTestHelper.CURSOR_OFFSET_LENGTH, '0');
        return new StreamBatch(
                new SubscriptionCursor(partition, paddedOffset, eventType, DUMMY_TOKEN),
                new JSONArray().put(event),
                metadata);
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
            final StreamBatch batchToCheck = (StreamBatch) item;
            final SubscriptionCursor cursor = batch.getCursor();
            final SubscriptionCursor cursorToCheck = batchToCheck.getCursor();

            return sameBatchEvents(batchToCheck.getEvents()) &&
                    cursor.getPartition().equals(cursorToCheck.getPartition()) &&
                    cursor.getOffset().equals(cursorToCheck.getOffset()) &&
                    cursor.getEventType().equals(cursorToCheck.getEventType()) &&
                    Optional.ofNullable(batch.getMetadata())
                            .map(b -> b.equals(batchToCheck.getMetadata()))
                            .orElse(batchToCheck.getMetadata() == null);
        }

        private boolean sameBatchEvents(final List<JSONObject> eventsToCheck) {
            final List<JSONObject> events = batch.getEvents();
            return events.size() == eventsToCheck.size() &&
                    IntStream.range(0, events.size())
                    .allMatch(i ->
                            events.get(i).toMap().equals(
                                    eventsToCheck.get(i).toMap()));
        }

        @Override
        public void describeTo(final Description description) {
        }

        public static MatcherIgnoringToken equalToBatchIgnoringToken(final StreamBatch batch) {
            return new MatcherIgnoringToken(batch);
        }
    }
}
