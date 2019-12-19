package org.zalando.nakadi.service.subscription.state;

import com.google.common.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.subscription.zk.ZkSubscription;
import org.zalando.nakadi.view.SubscriptionCursorWithoutToken;

import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Predicate;

class PartitionData {
    private final Comparator<NakadiCursor> comparator;
    private final ZkSubscription<SubscriptionCursorWithoutToken> subscription;
    private final List<ConsumedEvent> nakadiEvents = new LinkedList<>();
    private final NavigableSet<NakadiCursor> allCursorsOrdered;
    private final Logger log;

    private NakadiCursor commitOffset;
    private NakadiCursor sentOffset;
    private long lastSendMillis;
    private long batchWindowStartTimestamp;
    private int keepAliveInARow;
    private long bytesInMemory;
    final long batchTimespanMillis;

    @VisibleForTesting
    PartitionData(final Comparator<NakadiCursor> comparator,
                  final ZkSubscription<SubscriptionCursorWithoutToken> subscription,
                  final NakadiCursor commitOffset,
                  final long currentTime,
                  final long batchTimespanMillis) {
        this(comparator, subscription, commitOffset, LoggerFactory.getLogger(PartitionData.class), currentTime,
                batchTimespanMillis);
        bytesInMemory = 0L;
    }

    @VisibleForTesting
    PartitionData(final Comparator<NakadiCursor> comparator,
                  final ZkSubscription<SubscriptionCursorWithoutToken> subscription,
                  final NakadiCursor commitOffset,
                  final long currentTime) {
        this(comparator, subscription, commitOffset, LoggerFactory.getLogger(PartitionData.class), currentTime, 0L);
        bytesInMemory = 0L;
    }

    PartitionData(
            final Comparator<NakadiCursor> comparator,
            final ZkSubscription<SubscriptionCursorWithoutToken> subscription,
            final NakadiCursor commitOffset,
            final Logger log,
            final long currentTime,
            final long batchTimespanMillis) {
        this.batchTimespanMillis = batchTimespanMillis;
        this.comparator = comparator;
        this.allCursorsOrdered = new TreeSet<>(comparator);
        this.subscription = subscription;
        this.log = log;

        this.commitOffset = commitOffset;
        this.sentOffset = commitOffset;
        this.lastSendMillis = currentTime;
        this.batchWindowStartTimestamp = 0L;
    }

    @Nullable
    List<ConsumedEvent> takeEventsToStream(final long currentTimeMillis,
                                           final int batchSize, final long batchTimeoutMillis,
                                           final boolean streamTimeoutReached) {
        final boolean countReached = (nakadiEvents.size() >= batchSize) && batchSize > 0;
        final boolean timeReached = (currentTimeMillis - lastSendMillis) >= batchTimeoutMillis;

        if (batchTimespanMillis > 0 && lastRecordTimestamp() >= batchWindowEndTimestamp()) {
            lastSendMillis = currentTimeMillis;
            return extractTimespan(batchWindowEndTimestamp());
        } else if (countReached || timeReached) {
            lastSendMillis = currentTimeMillis;
            batchWindowStartTimestamp = lastSendMillis;
            return extractCount(batchSize);
        } else if (streamTimeoutReached) {
            lastSendMillis = currentTimeMillis;
            batchWindowStartTimestamp = lastSendMillis;
            final List<ConsumedEvent> extractedEvents = extractCount(batchSize);
            return extractedEvents.isEmpty() ? null : extractedEvents;
        } else {
            return null;
        }
    }

    private long batchWindowEndTimestamp() {
        if (batchWindowStartTimestamp == 0 && !nakadiEvents.isEmpty()) {
            batchWindowStartTimestamp = nakadiEvents.get(0).getTimestamp();
        }

        return batchWindowStartTimestamp + batchTimespanMillis;
    }

    private long lastRecordTimestamp() {
        if (nakadiEvents.size() > 0) {
            return nakadiEvents.get(nakadiEvents.size() - 1).getTimestamp();
        } else {
            return 0;
        }
    }

    private List<ConsumedEvent> extractTimespan(final long batchWindowEndTimestamp) {
        // extract at least one. This condition is necessary in case the event that triggers the extract is outside
        // the window but it's the only event to be streamed.
        final List<ConsumedEvent> events = extract((taken) -> nakadiEvents.get(0).getTimestamp() < batchWindowEndTimestamp || taken == 0);

        // needed to fast forward the window start in case there are no events for an extended period of time
        batchWindowStartTimestamp = Math.max(batchWindowEndTimestamp, events.get(events.size()-1).getTimestamp());

        return events;
    }

    NakadiCursor getSentOffset() {
        return sentOffset;
    }

    NakadiCursor getCommitOffset() {
        return commitOffset;
    }

    long getLastSendMillis() {
        return lastSendMillis;
    }

    long getBytesInMemory() {
        return bytesInMemory;
    }

    private List<ConsumedEvent> extractCount(final int count) {
        return extract((i) -> i < count);
    }

    private List<ConsumedEvent> extract(final Predicate<Integer> condition) {
        final List<ConsumedEvent> result = new ArrayList<>();
        for (int i = 0; !nakadiEvents.isEmpty() && condition.test(i); ++i) {
            final ConsumedEvent event = nakadiEvents.remove(0);
            bytesInMemory -= event.getEvent().length;
            result.add(event);
        }
        if (!result.isEmpty()) {
            this.sentOffset = result.get(result.size() - 1).getPosition();
            this.keepAliveInARow = 0;
        } else {
            this.keepAliveInARow += 1;
        }
        return result;
    }

    public List<ConsumedEvent> extractMaxEvents(final long currentTimeMillis, final int count) {
        final List<ConsumedEvent> result = extractCount(count);
        if(!result.isEmpty()) {
            lastSendMillis = currentTimeMillis;
        }
        return result;
    }

    int getKeepAliveInARow() {
        return keepAliveInARow;
    }

    /**
     * Ensures, that last commit and last send positions corresponds to offsets available in kafka.
     * The situation is possible whenever old subscriptions are stared. commit offset is N, but kafka
     * already deleted all the data with offsets > N (for example [N, M]). One need to start streaming with
     * new positions, and update commit offset as well (because it could happened that there are no messages to
     * stream according to window size)
     *
     * @param beforeFirst Position to check against (last inaccessible position in stream)
     */
    void ensureDataAvailable(final NakadiCursor beforeFirst) {
        if (comparator.compare(beforeFirst, commitOffset) > 0) {
            log.warn("Oldest kafka position is {} and commit offset is {}, updating", beforeFirst, commitOffset);
            commitOffset = beforeFirst;
        }
        if (comparator.compare(beforeFirst, sentOffset) > 0) {
            log.warn("Oldest kafka position is {} and sent offset is {}, updating", beforeFirst, sentOffset);
            sentOffset = beforeFirst;
        }
    }

    static class CommitResult {
        final boolean seekOnKafka;
        final long committedCount;

        private CommitResult(final boolean seekOnKafka, final long committedCount) {
            this.seekOnKafka = seekOnKafka;
            this.committedCount = committedCount;
        }
    }

    CommitResult onCommitOffset(final NakadiCursor offset) {
        boolean seekKafka = false;
        if (comparator.compare(offset, sentOffset) > 0) {
            log.error("Commit in future: current: {}, committed {} will skip sending obsolete data", sentOffset,
                    commitOffset);
            seekKafka = true;
            sentOffset = offset;
        }
        final long committed;
        if (comparator.compare(offset, commitOffset) >= 0) {
            final Set<NakadiCursor> committedCursors = allCursorsOrdered.headSet(offset, true);
            committed = committedCursors.size();
            commitOffset = offset;
            // Operation is cascaded to allCursorsOrdered set.
            committedCursors.clear();
        } else {
            log.error("Commits in past are evil!: Committing in {} while current commit is {}", offset, commitOffset);
            // Commit in past occurred. One should move storage pointer to sentOffset.
            seekKafka = true;
            commitOffset = offset;
            sentOffset = commitOffset;
            allCursorsOrdered.clear();
            nakadiEvents.clear();
            bytesInMemory = 0L;
            committed = 0;
        }
        while (!nakadiEvents.isEmpty() && comparator.compare(nakadiEvents.get(0).getPosition(), commitOffset) <= 0) {
            final ConsumedEvent evt = nakadiEvents.remove(0);
            bytesInMemory -= evt.getEvent().length;
        }
        return new CommitResult(seekKafka, committed);
    }

    void addEvent(final ConsumedEvent event) {
        nakadiEvents.add(event);
        bytesInMemory += event.getEvent().length;
        allCursorsOrdered.add(event.getPosition());
    }

    boolean isCommitted() {
        return comparator.compare(sentOffset, commitOffset) <= 0;
    }

    int getUnconfirmed() {
        return allCursorsOrdered.headSet(sentOffset, true).size();
    }

    public ZkSubscription<SubscriptionCursorWithoutToken> getSubscription() {
        return subscription;
    }
}
