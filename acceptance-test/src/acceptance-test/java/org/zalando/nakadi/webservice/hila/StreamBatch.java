package org.zalando.nakadi.webservice.hila;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.StringUtils;
import org.hamcrest.Matcher;
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

import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.beans.HasPropertyWithValue.hasProperty;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;
import static org.hamcrest.collection.IsEmptyIterable.emptyIterable;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static uk.co.datumedge.hamcrest.json.SameJSONAs.sameJSONObjectAs;

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

    public static Matcher<StreamBatch> equalToBatchIgnoringToken(final StreamBatch batch) {
        final SubscriptionCursor cursor = batch.getCursor();
        final List<JSONObject> events = batch.getEvents();
        final StreamMetadata metadata = batch.getMetadata();
        return allOf(
                isA(StreamBatch.class),
                hasProperty("cursor", allOf(
                                hasProperty("partition", equalTo(cursor.getPartition())),
                                hasProperty("offset",    equalTo(cursor.getOffset())),
                                hasProperty("eventType", equalTo(cursor.getEventType())))),
                hasProperty("events", hasSize(events.size())),
                hasProperty("events", anyOf(
                                emptyIterable(), // otherwise it fails if `events` is empty
                                contains(events.stream()
                                        .map(e -> sameJSONObjectAs(e))
                                        .collect(Collectors.toList())))),
                hasProperty("metadata", equalTo(metadata)));
    }
}
