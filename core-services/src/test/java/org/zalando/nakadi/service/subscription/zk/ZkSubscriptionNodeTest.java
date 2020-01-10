package org.zalando.nakadi.service.subscription.zk;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;
import org.zalando.nakadi.service.subscription.zk.ZkSubscriptionNode;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.AUTO;
import static org.zalando.nakadi.domain.SubscriptionEventTypeStats.Partition.AssignmentType.DIRECT;

public class ZkSubscriptionNodeTest {

    private ZkSubscriptionNode zkSubscriptionNode;

    @Before
    public void before() {
        final List<Partition> partitions = ImmutableList.of(
                new Partition("et1", "0", "stream1", null, Partition.State.ASSIGNED),
                new Partition("et1", "1", "stream2", "stream4", Partition.State.REASSIGNING),
                new Partition("et2", "0", "stream3", null, Partition.State.UNASSIGNED),
                new Partition("et2", "1", null, null, null)
        );

        final List<Session> sessions = ImmutableList.of(
                new Session("stream1", 1, ImmutableList.of(new EventTypePartition("et1", "0"))),
                new Session("stream2", 1),
                new Session("stream3", 1),
                new Session("stream4", 1)
        );

        zkSubscriptionNode = new ZkSubscriptionNode(partitions, sessions);
    }

    @Test
    public void whenGuessStreamThenOk() {
        assertThat(zkSubscriptionNode.guessStream("et1", "0"), equalTo("stream1"));
        assertThat(zkSubscriptionNode.guessStream("et1", "1"), equalTo("stream2"));
        assertThat(zkSubscriptionNode.guessStream("et2", "0"), equalTo("stream3"));
        assertThat(zkSubscriptionNode.guessStream("et2", "1"), equalTo(null));
    }

    @Test
    public void whenGuessStateThenOk() {
        assertThat(zkSubscriptionNode.guessState("et1", "0"), equalTo(Partition.State.ASSIGNED));
        assertThat(zkSubscriptionNode.guessState("et1", "1"), equalTo(Partition.State.REASSIGNING));
        assertThat(zkSubscriptionNode.guessState("et2", "0"), equalTo(Partition.State.UNASSIGNED));
        assertThat(zkSubscriptionNode.guessState("et2", "1"), equalTo(Partition.State.UNASSIGNED));
    }

    @Test
    public void whenGetPartitionAssignmentTypeThenOk() {
        assertThat(zkSubscriptionNode.getPartitionAssignmentType("et1", "0"), equalTo(DIRECT));
        assertThat(zkSubscriptionNode.getPartitionAssignmentType("et1", "1"), equalTo(AUTO));
        assertThat(zkSubscriptionNode.getPartitionAssignmentType("et2", "0"), equalTo(AUTO));
        assertThat(zkSubscriptionNode.getPartitionAssignmentType("et2", "1"), equalTo(null));
    }
}
