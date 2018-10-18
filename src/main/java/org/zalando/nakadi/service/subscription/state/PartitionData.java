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

class PartitionData {
    private final Comparator<NakadiCursor> comparator;
    private final ZkSubscription<SubscriptionCursorWithoutToken> subscription;
    private final List<ConsumedEvent> nakadiEvents = new LinkedList<>();
    private final NavigableSet<NakadiCursor> allCursorsOrdered;
    private final Logger log;

    private NakadiCursor commitOffset;
    private NakadiCursor sentOffset;
    private long lastSendMillis;
    private int keepAliveInARow;
    private long bytesInMemory;

    @VisibleForTesting
    PartitionData(final Comparator<NakadiCursor> comparator,
                  final ZkSubscription<SubscriptionCursorWithoutToken> subscription, final NakadiCursor commitOffset,
                  final long currentTime) {
        this(comparator, subscription, commitOffset, LoggerFactory.getLogger(PartitionData.class), currentTime);
        bytesInMemory = 0L;
    }

    PartitionData(
            final Comparator<NakadiCursor> comparator,
            final ZkSubscription<SubscriptionCursorWithoutToken> subscription,
            final NakadiCursor commitOffset,
            final Logger log,
            final long currentTime) {
        this.comparator = comparator;
        this.allCursorsOrdered = new TreeSet<>(comparator);
        this.subscription = subscription;
        this.log = log;

        this.commitOffset = commitOffset;
        this.sentOffset = commitOffset;
        this.lastSendMillis = currentTime;
    }

    @Nullable
    List<ConsumedEvent> takeEventsToStream(final long currentTimeMillis, final int batchSize,
                                           final long batchTimeoutMillis, final boolean streamTimeoutReached) {
        final boolean countReached = (nakadiEvents.size() >= batchSize) && batchSize > 0;
        final boolean timeReached = (currentTimeMillis - lastSendMillis) >= batchTimeoutMillis;
        if (countReached || timeReached) {
            lastSendMillis = currentTimeMillis;
            return extract(batchSize);
        } else if (streamTimeoutReached) {
            lastSendMillis = currentTimeMillis;
            final List<ConsumedEvent> extractedEvents = extract(batchSize);
            return extractedEvents.isEmpty() ? null : extractedEvents;
        } else {
            return null;
        }
    }

    public List<ConsumedEvent> extractAll(final long currentTimeMillis) {
        final List<ConsumedEvent> result = extract(nakadiEvents.size());
        if (!result.isEmpty()) {
            lastSendMillis = currentTimeMillis;
        }
        return result;
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

    private List<ConsumedEvent> extract(final int count) {
        final List<ConsumedEvent> result = new ArrayList<>();
        for (int i = 0; i < count && !nakadiEvents.isEmpty(); ++i) {
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
        final List<ConsumedEvent> result = extract(count);
        if(!result.isEmpty()) {
            lastSendMillis = currentTimeMillis;
        }
        return result;
    }

    public int getNumberOfUnsentEvents() {
        return this.nakadiEvents.size();
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
