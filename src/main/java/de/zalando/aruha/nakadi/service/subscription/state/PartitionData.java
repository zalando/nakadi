package de.zalando.aruha.nakadi.service.subscription.state;

import de.zalando.aruha.nakadi.service.subscription.zk.ZKSubscription;
import java.util.NavigableMap;
import java.util.SortedMap;
import java.util.TreeMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class PartitionData {
    private final ZKSubscription subscription;
    private final NavigableMap<Long, String> nakadiEvents = new TreeMap<>();
    private final Logger log;

    private long commitOffset;
    private long sentOffset;
    private long lastSendMillis;
    private int keepAliveInARow;

    PartitionData(final ZKSubscription subscription, final Long commitOffset) {
        this(subscription, commitOffset, LoggerFactory.getLogger(PartitionData.class));
    }

    PartitionData(final ZKSubscription subscription, final Long commitOffset, final Logger log) {
        this.subscription = subscription;
        this.log = log;

        this.commitOffset = commitOffset;
        this.sentOffset = commitOffset;
        this.lastSendMillis = System.currentTimeMillis();
    }

    SortedMap<Long, String> takeEventsToStream(final long currentTimeMillis, final int batchSize, final long batchTimeoutMillis) {
        final boolean countReached = (nakadiEvents.size() >= batchSize) && batchSize > 0;
        final boolean timeReached = (currentTimeMillis - lastSendMillis) >= batchTimeoutMillis;
        if (countReached || timeReached) {
            lastSendMillis = currentTimeMillis;
            return extract(batchSize);
        } else {
            return null;
        }
    }

    long getSentOffset() {
        return sentOffset;
    }

    long getLastSendMillis() {
        return lastSendMillis;
    }

    private SortedMap<Long, String> extract(final int count) {
        final SortedMap<Long, String> result = new TreeMap<>();
        for (int i = 0; i < count && !nakadiEvents.isEmpty(); ++i) {
            final Long offset = nakadiEvents.firstKey();
            result.put(offset, nakadiEvents.remove(offset));
        }
        if (!result.isEmpty()) {
            this.sentOffset = result.lastKey();
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
     * @param position First position available in kafka
     */
    void ensureDataAvailable(final long position) {
        if (position > commitOffset + 1) {
            log.warn("Oldest kafka position is {} and commit offset is {}, updating", position, commitOffset);
            commitOffset = position;
        }
        if (position > sentOffset + 1) {
            log.warn("Oldest kafka position is {} and sent offset is {}, updating", position, commitOffset);
            sentOffset = position;
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

    CommitResult onCommitOffset(final Long offset) {
        boolean seekKafka = false;
        if (offset > sentOffset) {
            log.error("Commit in future: current: {}, committed {} will skip sending obsolete data", sentOffset, commitOffset);
            seekKafka = true;
            sentOffset = offset;
        }
        final long committed;
        if (offset >= commitOffset) {
            committed = offset - commitOffset;
            commitOffset = offset;
        } else {
            log.error("Commits in past are evil!: Committing in {} while current commit is {}", offset, commitOffset);
            seekKafka = true;
            commitOffset = offset;
            sentOffset = commitOffset;
            committed = 0;
        }
        while (nakadiEvents.floorKey(commitOffset) != null) {
            nakadiEvents.pollFirstEntry();
        }
        return new CommitResult(seekKafka, committed);
    }

    void addEventFromKafka(final long offset, final String event) {
        if (offset > (sentOffset + nakadiEvents.size() + 1)) {
            log.warn(
                    "Adding event from kafka that is too far from last sent. " +
                            "Dunno how it happened, but it is. Sent offset: {}, Commit offset: {}, Adding offset: {}",
                    sentOffset, commitOffset, offset);
        }
        nakadiEvents.put(offset, event);
    }

    void clearEvents() {
        nakadiEvents.clear();
    }

    boolean isCommitted() {
        return sentOffset <= commitOffset;
    }

    long getUnconfirmed() {
        return sentOffset - commitOffset;
    }

    public ZKSubscription getSubscription() {
        return subscription;
    }
}
