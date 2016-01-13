package de.zalando.aruha.nakadi.domain;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * This is just a dummy that will be replaced when we finalize the high level consuming architecture
 */
public class Subscription {

    private String subscriptionId;

    /**
     * topics to consume from
     */
    private List<String> topics;

    /**
     * this keeps the committed offsets
     */
    private List<Cursor> cursors;

    public Subscription(final String subscriptionId, final List<String> topics) {
        this.subscriptionId = subscriptionId;
        this.topics = ImmutableList.copyOf(topics);
        cursors = Lists.newArrayList();
    }

    public String getSubscriptionId() {
        return subscriptionId;
    }

    public List<String> getTopics() {
        return topics;
    }

    public List<Cursor> getCursors() {
        return Collections.unmodifiableList(cursors);
    }

    public Optional<Cursor> getCursor(final TopicPartition tp) {
        return cursors
                .stream()
                .filter(cursor ->
                        cursor.getTopic().equals(tp.getTopic()) && cursor.getPartition().equals(tp.getPartition()))
                .findFirst();
    }

    public void updateCursor(String topic, String partition, String offset) {
        cursors
                .stream()
                .filter(cursor -> cursor.getPartition().equals(partition) && cursor.getTopic().equals(topic))
                .findFirst()
                .orElseGet(() -> {
                    final Cursor newCursor = new Cursor(topic, partition, offset);
                    cursors.add(newCursor);
                    return newCursor;
                })
                .setOffset(offset);
    }
}
