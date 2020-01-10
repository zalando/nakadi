package org.zalando.nakadi.service.subscription;

import com.google.common.collect.ImmutableList;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypePartition;
import org.zalando.nakadi.service.subscription.model.Partition;
import org.zalando.nakadi.service.subscription.model.Session;

import java.util.List;
import java.util.stream.Stream;

import static com.google.common.collect.Sets.newHashSet;
import static org.hamcrest.Matchers.emptyArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.zalando.nakadi.service.subscription.model.Partition.State.ASSIGNED;
import static org.zalando.nakadi.service.subscription.model.Partition.State.REASSIGNING;
import static org.zalando.nakadi.service.subscription.model.Partition.State.UNASSIGNED;

public class SubscriptionRebalancerTest {

    @Test(expected = IllegalArgumentException.class)
    public void splitByWeightShouldAcceptOnlyCorrectData1() {
        SubscriptionRebalancer.splitByWeight(1, new int[]{1, 1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void splitByWeightShouldAcceptOnlyCorrectData2() {
        SubscriptionRebalancer.splitByWeight(2, new int[]{1, 0});
    }

    @Test
    public void splitByWeightMustCorrectlyWorkOnDifferentValues() {
        assertArrayEquals(
                new int[]{1, 1},
                SubscriptionRebalancer.splitByWeight(2, new int[]{1, 1}));
        assertArrayEquals(
                new int[]{2, 1},
                SubscriptionRebalancer.splitByWeight(3, new int[]{1, 1}));
        assertArrayEquals(
                new int[]{1, 2},
                SubscriptionRebalancer.splitByWeight(3, new int[]{1, 2}));
        assertArrayEquals(
                new int[]{34, 33, 33},
                SubscriptionRebalancer.splitByWeight(100, new int[]{1, 1, 1}));
        assertArrayEquals(
                new int[]{26, 25, 50},
                SubscriptionRebalancer.splitByWeight(101, new int[]{1, 1, 2}));
    }

    @Test
    public void directlyRequestedPartitionsAreCaptured() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(
                        new Session("s1", 1),
                        new Session("s2", 1),
                        new Session("s3", 1, ImmutableList.of(
                                new EventTypePartition("et1", "p1"),
                                new EventTypePartition("et1", "p4")))),
                new Partition[]{
                        new Partition("et1", "p1", "s7", null, ASSIGNED),
                        new Partition("et1", "p2", "s7", null, ASSIGNED),
                        new Partition("et1", "p3", "s7", null, ASSIGNED),
                        new Partition("et1", "p4", "s7", null, ASSIGNED)});

        assertEquals(newHashSet(changeset), newHashSet(
                new Partition("et1", "p1", "s3", null, ASSIGNED),
                new Partition("et1", "p2", "s1", null, ASSIGNED),
                new Partition("et1", "p3", "s2", null, ASSIGNED),
                new Partition("et1", "p4", "s3", null, ASSIGNED)));
    }

    @Test
    public void directlyAssignedPartitionsAreNotTransferred() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(
                        new Session("s1", 1, ImmutableList.of(new EventTypePartition("et1", "p1"))),
                        new Session("s2", 1)),
                new Partition[]{
                        new Partition("et1", "p1", "s1", null, ASSIGNED),
                        new Partition("et1", "p2", null, null, UNASSIGNED)});

        assertEquals(newHashSet(changeset), newHashSet(
                new Partition("et1", "p2", "s2", null, ASSIGNED)));
    }

    @Test
    public void directlyRequestedPartitionsAreTransferred() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(
                        new Session("s1", 1),
                        new Session("s2", 1),
                        new Session("s3", 1, ImmutableList.of(
                                new EventTypePartition("et1", "p1"),
                                new EventTypePartition("et1", "p4")))),
                new Partition[]{
                        new Partition("et1", "p1", "s1", null, ASSIGNED),
                        new Partition("et1", "p2", "s1", null, ASSIGNED),
                        new Partition("et1", "p3", "s2", null, ASSIGNED),
                        new Partition("et1", "p4", "s2", null, ASSIGNED)});

        assertEquals(newHashSet(changeset), newHashSet(
                new Partition("et1", "p1", "s1", "s3", REASSIGNING),
                new Partition("et1", "p4", "s2", "s3", REASSIGNING)));
    }

    @Test
    public void onlyDirectSessionsWorkFine() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(
                        new Session("s1", 1, ImmutableList.of(new EventTypePartition("et1", "p3"))),
                        new Session("s2", 1, ImmutableList.of(new EventTypePartition("et1", "p2")))),
                new Partition[]{
                        new Partition("et1", "p1", null, null, UNASSIGNED),
                        new Partition("et1", "p2", null, null, UNASSIGNED),
                        new Partition("et1", "p3", null, null, UNASSIGNED),
                        new Partition("et1", "p4", null, null, UNASSIGNED)});

        assertEquals(newHashSet(changeset), newHashSet(
                new Partition("et1", "p3", "s1", null, ASSIGNED),
                new Partition("et1", "p2", "s2", null, ASSIGNED)));
    }

    @Test
    public void rebalanceShouldHaveEmptyChangesetForBalancedData() {
        final SubscriptionRebalancer rebalancer = new SubscriptionRebalancer();

        // 1. Data contains only assigned
        final List<Session> sessions = ImmutableList.of(
                new Session("0", 1),
                new Session("1", 1));
        assertThat(rebalancer.apply(sessions,
                new Partition[]{
                        new Partition("0", "0", "0", null, ASSIGNED),
                        new Partition("0", "1", "1", null, ASSIGNED),
                        new Partition("1", "0", "1", null, ASSIGNED),
                        new Partition("1", "1", "0", null, ASSIGNED)}),
                emptyArray());

        // 2. Data contains reassinging
        assertThat(rebalancer.apply(sessions,
                new Partition[]{
                        new Partition("0", "0", "0", null, ASSIGNED),
                        new Partition("0", "1", "0", "1", REASSIGNING),
                        new Partition("1", "0", "1", null, ASSIGNED),
                        new Partition("1", "1", "0", null, ASSIGNED)}),
                emptyArray());

        // 3. Data contains only reassinging
        assertThat(rebalancer.apply(sessions,
                new Partition[]{
                        new Partition("0", "0", "0", "1", REASSIGNING),
                        new Partition("0", "1", "0", "1", REASSIGNING),
                        new Partition("1", "0", "1", "0", REASSIGNING),
                        new Partition("1", "1", "1", "0", REASSIGNING)}),
                emptyArray());
    }

    @Test
    public void rebalanceShouldRemoveDeadSessions() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(new Session("1", 1), new Session("2", 1)),
                new Partition[]{
                        new Partition("0", "0", "0", null, ASSIGNED),
                        new Partition("0", "1", "0", "1", REASSIGNING),
                        new Partition("1", "0", "1", null, ASSIGNED),
                        new Partition("1", "1", "0", null, ASSIGNED)});

        assertEquals(3, changeset.length);
        // All partitions must be in assigned state
        assertFalse(Stream.of(changeset).anyMatch(p -> p.getState() != ASSIGNED));
        // All partitions must not have nextSessionId
        assertFalse(Stream.of(changeset).anyMatch(p -> p.getNextSession() != null));

        assertFalse(Stream.of(changeset).anyMatch(p -> p.getKey().equals(new EventTypePartition("1", "0"))));
        assertEquals(1, Stream.of(changeset).filter(p -> p.getSession().equals("1")).count());
        assertEquals(2, Stream.of(changeset).filter(p -> p.getSession().equals("2")).count());
        assertFalse(Stream.of(changeset).anyMatch(p -> p.getState() != ASSIGNED));
    }

    @Test
    public void rebalanceShouldMoveToReassigningState() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(new Session("1", 1), new Session("2", 1), new Session("3", 1)),
                new Partition[]{
                        new Partition("0", "0", "1", null, ASSIGNED),
                        new Partition("0", "1", "1", null, ASSIGNED),
                        new Partition("1", "0", "2", null, ASSIGNED),
                        new Partition("1", "1", "2", null, ASSIGNED)});
        assertEquals(1, changeset.length);
        final Partition changed = changeset[0];
        assertTrue(
                changed.getKey().equals(new EventTypePartition("1", "0")) ||
                        changed.getKey().equals(new EventTypePartition("1", "1")));
        assertEquals("2", changed.getSession());
        assertEquals("3", changed.getNextSession());
        assertEquals(REASSIGNING, changed.getState());
    }

    @Test
    public void rebalanceShouldTakeRebalancingPartitions() {
        final Partition[] changeset = new SubscriptionRebalancer().apply(
                ImmutableList.of(new Session("1", 1), new Session("2", 1), new Session("3", 1)),
                new Partition[]{
                        new Partition("0", "0", "1", null, ASSIGNED),
                        new Partition("0", "1", "1", null, ASSIGNED),
                        new Partition("1", "0", "2", null, REASSIGNING),
                        new Partition("1", "1", "2", null, ASSIGNED)});
        assertEquals(1, changeset.length);
        final Partition changed = changeset[0];
        assertEquals(new EventTypePartition("1", "0"), changed.getKey());
        assertEquals("2", changed.getSession());
        assertEquals("3", changed.getNextSession());
        assertEquals(REASSIGNING, changed.getState());
    }
}
