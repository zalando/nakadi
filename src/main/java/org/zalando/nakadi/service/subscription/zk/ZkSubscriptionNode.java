package org.zalando.nakadi.service.subscription.zk;

import java.util.Optional;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

public final class ZkSubscriptionNode {

    private Partition[] partitions;
    private Session[] sessions;

    public ZkSubscriptionNode() {
        this.partitions = new Partition[0];
        this.sessions = new Session[0];
    }

    public ZkSubscriptionNode(final Partition[] partitions, final Session[] sessions) {
        this.partitions = partitions;
        this.sessions = sessions;
    }

    public void setPartitions(final Partition[] partitions) {
        this.partitions = partitions;
    }

    public void setSessions(final Session[] sessions) {
        this.sessions = sessions;
    }

    public Partition[] getPartitions() {
        return partitions;
    }

    public Session[] getSessions() {
        return sessions;
    }

    public Partition.State guessState(final String partition) {
        return getPartitionWithActiveSession(partition).map(Partition::getState).orElse(Partition.State.UNASSIGNED);
    }

    private Optional<Partition> getPartitionWithActiveSession(final String partition) {
        return Stream.of(partitions)
                .filter(p -> p.getPartition().equals(partition))
                .filter(p -> Stream.of(sessions).anyMatch(s -> s.getId().equalsIgnoreCase(p.getSession())))
                .findAny();
    }

    @Nullable
    public String guessStream(final String partition) {
        return getPartitionWithActiveSession(partition).map(Partition::getSession).orElse(null);
    }
}
