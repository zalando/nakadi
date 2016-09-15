package org.zalando.nakadi.service.subscription.zk;

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
}
