package org.zalando.nakadi.service.subscription.autocommit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.CursorConverter;
import org.zalando.nakadi.service.CursorOperationsService;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionClient;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AutocommitSupport {
    private final CursorOperationsService cursorOperationsService;
    private final ZkSubscriptionClient zkSubscriptionClient;
    private final Map<EventTypePartition, PartitionSkippedCursorsOperator> partitionsState = new HashMap<>();
    private final CursorConverter cursorConverter;

    private static final Logger LOG = LoggerFactory.getLogger(AutocommitSupport.class);

    public AutocommitSupport(
            final CursorOperationsService cursorOperationsService,
            final ZkSubscriptionClient zkSubscriptionClient,
            final CursorConverter cursorConverter) {
        this.cursorOperationsService = cursorOperationsService;
        this.zkSubscriptionClient = zkSubscriptionClient;
        this.cursorConverter = cursorConverter;
    }

    public void addPartition(final NakadiCursor committed) {
        if (partitionsState.containsKey(committed.getEventTypePartition())) {
            return;
        }
        partitionsState.put(
                committed.getEventTypePartition(),
                new PartitionSkippedCursorsOperator(cursorOperationsService, committed));
    }

    public void removePartition(final EventTypePartition eventTypePartition) {
        try {
            autocommit();
        } catch (RuntimeException ex) {
            LOG.warn("Failed to execute autocommit while removing partition {}", eventTypePartition, ex);
        }
        partitionsState.remove(eventTypePartition);
    }

    public void addSkippedEvent(final NakadiCursor cursor) {
        final PartitionSkippedCursorsOperator partitionSkippedCursorsOperator =
                partitionsState.get(cursor.getEventTypePartition());
        if (null != partitionSkippedCursorsOperator) {
            partitionSkippedCursorsOperator.addSkippedEvent(cursor);
        }
    }

    public void onCommit(final NakadiCursor cursor) {
        final PartitionSkippedCursorsOperator partitionSkippedCursorsOperator =
                partitionsState.get(cursor.getEventTypePartition());
        if (null != partitionSkippedCursorsOperator) {
            partitionSkippedCursorsOperator.onCommit(cursor);
        }
    }

    // As skipped events are not participating in memory consumption or any other stuff, we can safely call
    // this method as often as we want (once a second, once a minute or whatever), consumption of main (non-skipped)
    // data is not slowing down because of autocommit not being called.
    // The only limitation is about monitoring - the less times it is called -> the less accurate monitoring is.
    public void autocommit() {
        List<NakadiCursor> toAutocommit = null;
        for (final PartitionSkippedCursorsOperator state : partitionsState.values()) {
            final NakadiCursor c = state.getAutoCommitSuggestion();
            if (null == c) {
                continue;
            }
            if (null == toAutocommit) {
                toAutocommit = new ArrayList<>();
            }
            toAutocommit.add(c);
        }
        if (null == toAutocommit) {
            return;
        }

        final List<SubscriptionCursorWithoutToken> converted = toAutocommit.stream()
                .map(cursorConverter::convertToNoToken)
                .collect(Collectors.toList());
        zkSubscriptionClient.commitOffsets(converted);
    }
}
