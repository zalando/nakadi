package org.zalando.nakadi.service.subscription.state;

import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.NavigableSet;
import java.util.Set;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zalando.nakadi.domain.ConsumedEvent;
import org.zalando.nakadi.domain.NakadiCursor;
import org.zalando.nakadi.service.subscription.zk.ZKSubscription;

class PartitionData {
    private final ZKSubscription subscription;
    private final List<ConsumedEvent> nakadiEvents = new LinkedList<>();
    private final NavigableSet<NakadiCursor> allCursorsOrdered = new TreeSet<>();
    private final Logger log;

    private NakadiCursor commitOffset;
    private NakadiCursor sentOffset;
    private long lastSendMillis;
    private int keepAliveInARow;

    @VisibleForTesting
    PartitionData(final ZKSubscription subscription, final NakadiCursor commitOffset) {
        this(subscription, commitOffset, LoggerFactory.getLogger(PartitionData.class));
    }

    PartitionData(final ZKSubscription subscription, final NakadiCursor commitOffset, final Logger log) {
        this.subscription = subscription;
        this.log = log;

        this.commitOffset = commitOffset;
        this.sentOffset = commitOffset;
        this.lastSendMillis = System.currentTimeMillis();
    }

    @Nullable
    List<ConsumedEvent> takeEventsToStream(final long currentTimeMillis, final int batchSize,
                                           final long batchTimeoutMillis) {
        final boolean countReached = (nakadiEvents.size() >= batchSize) && batchSize > 0;
        final boolean timeReached = (currentTimeMillis - lastSendMillis) >= batchTimeoutMillis;
        if (countReached || timeReached) {
            lastSendMillis = currentTimeMillis;
            return extract(batchSize);
        } else {
            return null;
        }
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

    private List<ConsumedEvent> extract(final int count) {
        final List<ConsumedEvent> result = new ArrayList<>(count);
        for (int i = 0; i < count && !nakadiEvents.isEmpty(); ++i) {
            result.add(nakadiEvents.remove(0));
        }
        if (!result.isEmpty()) {
            this.sentOffset = result.get(result.size() - 1).getPosition();
            this.keepAliveInARow = 0;
        } else {
            this.keepAliveInARow += 1;
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
     * @param beforeFirst Position that
     */
    void ensureDataAvailable(final NakadiCursor beforeFirst) {
        if (beforeFirst.compareTo(commitOffset) > 0) {
            log.warn("Oldest kafka position is {} and commit offset is {}, updating", beforeFirst, commitOffset);
            commitOffset = beforeFirst;
        }
        if (beforeFirst.compareTo(sentOffset) > 0) {
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
        if (offset.compareTo(sentOffset) > 0) {
            log.error("Commit in future: current: {}, committed {} will skip sending obsolete data", sentOffset,
                    commitOffset);
            seekKafka = true;
            sentOffset = offset;
        }
        final long committed;
        if (offset.compareTo(commitOffset) >= 0) {
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
            committed = 0;
        }
        while (!nakadiEvents.isEmpty() && nakadiEvents.get(0).getPosition().compareTo(commitOffset) <= 0) {
            nakadiEvents.remove(0);
        }
        return new CommitResult(seekKafka, committed);
    }

    void addEvent(final ConsumedEvent event) {
        nakadiEvents.add(event);
        allCursorsOrdered.add(event.getPosition());
    }

    boolean isCommitted() {
        return sentOffset.compareTo(commitOffset) <= 0;
    }

    int getUnconfirmed() {
        return allCursorsOrdered.headSet(sentOffset, true).size();
    }

    public ZKSubscription getSubscription() {
        return subscription;
    }
}
