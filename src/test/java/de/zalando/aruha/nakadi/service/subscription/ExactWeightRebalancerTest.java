package de.zalando.aruha.nakadi.service.subscription;

import de.zalando.aruha.nakadi.service.subscription.model.Partition;
import de.zalando.aruha.nakadi.service.subscription.model.Session;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;
import org.junit.Test;

public class ExactWeightRebalancerTest {

    @Test(expected = IllegalArgumentException.class)
    public void splitByWeightShouldAcceptOnlyCorrectData1() {
        ExactWeightRebalancer.splitByWeight(1, new int[]{1, 1});
    }

    @Test(expected = IllegalArgumentException.class)
    public void splitByWeightShouldAcceptOnlyCorrectData2() {
        ExactWeightRebalancer.splitByWeight(2, new int[]{1, 0});
    }

    @Test
    public void splitByWeightMustCorrectlyWorkOnDifferentValues() {
        Assert.assertArrayEquals(
                new int[]{1, 1},
                ExactWeightRebalancer.splitByWeight(2, new int[]{1, 1}));
        Assert.assertArrayEquals(
                new int[]{2, 1},
                ExactWeightRebalancer.splitByWeight(3, new int[]{1, 1}));
        Assert.assertArrayEquals(
                new int[]{1, 2},
                ExactWeightRebalancer.splitByWeight(3, new int[]{1, 2}));
        Assert.assertArrayEquals(
                new int[]{34, 33, 33},
                ExactWeightRebalancer.splitByWeight(100, new int[]{1, 1, 1}));
        Assert.assertArrayEquals(
                new int[]{26, 25, 50},
                ExactWeightRebalancer.splitByWeight(101, new int[]{1, 1, 2}));
    }

    @Test
    public void rebalanceShouldHaveEmptyChangesetForBalancedData() {
        final ExactWeightRebalancer rebalancer = new ExactWeightRebalancer();

        // 1. Data contains only assigned
        final Session[] sessions = new Session[]{
                new Session("0", 1),
                new Session("1", 1)};
        Assert.assertNull(rebalancer.apply(sessions,
                new Partition[] {
                        new Partition(new Partition.PartitionKey("0", "0"), "0", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("0", "1"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "0"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "1"), "0", null, Partition.State.ASSIGNED)}));

        // 2. Data contains reassinging
        Assert.assertNull(rebalancer.apply(sessions,
                new Partition[] {
                        new Partition(new Partition.PartitionKey("0", "0"), "0", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("0", "1"), "0", "1", Partition.State.REASSIGNING),
                        new Partition(new Partition.PartitionKey("1", "0"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "1"), "0", null, Partition.State.ASSIGNED)}));

        // 3. Data contains only reassinging
        Assert.assertNull(rebalancer.apply(sessions,
                new Partition[] {
                        new Partition(new Partition.PartitionKey("0", "0"), "0", "1", Partition.State.REASSIGNING),
                        new Partition(new Partition.PartitionKey("0", "1"), "0", "1", Partition.State.REASSIGNING),
                        new Partition(new Partition.PartitionKey("1", "0"), "1", "0", Partition.State.REASSIGNING),
                        new Partition(new Partition.PartitionKey("1", "1"), "1", "0", Partition.State.REASSIGNING)}));
    }

    @Test
    public void rebalanceShouldRemoveDeadSessions() {
        final Partition[] changeset = new ExactWeightRebalancer().apply(
                new Session[]{new Session("1", 1), new Session("2", 1)},
                new Partition[] {
                        new Partition(new Partition.PartitionKey("0", "0"), "0", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("0", "1"), "0", "1", Partition.State.REASSIGNING),
                        new Partition(new Partition.PartitionKey("1", "0"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "1"), "0", null, Partition.State.ASSIGNED)});

        Assert.assertNotNull(changeset);
        Assert.assertEquals(3, changeset.length);
        // All partitions must be in assigned state
        Assert.assertFalse(Stream.of(changeset).filter(p -> p.getState() != Partition.State.ASSIGNED).findAny().isPresent());
        // All partitions must not have nextSessionId
        Assert.assertFalse(Stream.of(changeset).filter(p -> p.getNextSession() != null).findAny().isPresent());

        final Map<Partition.PartitionKey, Partition> mapped = Stream.of(changeset).collect(Collectors.toMap(Partition::getKey, p -> p));
        Assert.assertFalse(Stream.of(changeset).filter(p->p.getKey().equals(new Partition.PartitionKey("1", "0"))).findAny().isPresent());
        Assert.assertEquals(1, Stream.of(changeset).filter(p -> p.getSession().equals("1")).count());
        Assert.assertEquals(2, Stream.of(changeset).filter(p -> p.getSession().equals("2")).count());
        Assert.assertFalse(Stream.of(changeset).filter(p -> p.getState() != Partition.State.ASSIGNED).findAny().isPresent());
    }

    @Test
    public void rebalanceShouldMoveToReassigningState() {
        final Partition[] changeset = new ExactWeightRebalancer().apply(
                new Session[]{new Session("1", 1), new Session("2", 1), new Session("3", 1)},
                new Partition[] {
                        new Partition(new Partition.PartitionKey("0", "0"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("0", "1"), "1", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "0"), "2", null, Partition.State.ASSIGNED),
                        new Partition(new Partition.PartitionKey("1", "1"), "2", null, Partition.State.ASSIGNED)});
        Assert.assertNotNull(changeset);
        Assert.assertEquals(1, changeset.length);
        final Partition changed = changeset[0];
        Assert.assertTrue(changed.getKey().equals(new Partition.PartitionKey("1", "0")) || changed.getKey().equals(new Partition.PartitionKey("1", "1")));
        Assert.assertEquals("2", changed.getSession());
        Assert.assertEquals("3", changed.getNextSession());
        Assert.assertEquals(Partition.State.REASSIGNING, changed.getState());
    }

    @Test
    public void rebalanceShoultTakeRebalancingPartitions() {
        {
            final Partition[] changeset = new ExactWeightRebalancer().apply(
                    new Session[]{new Session("1", 1), new Session("2", 1), new Session("3", 1)},
                    new Partition[] {
                            new Partition(new Partition.PartitionKey("0", "0"), "1", null, Partition.State.ASSIGNED),
                            new Partition(new Partition.PartitionKey("0", "1"), "1", null, Partition.State.ASSIGNED),
                            new Partition(new Partition.PartitionKey("1", "0"), "2", null, Partition.State.REASSIGNING),
                            new Partition(new Partition.PartitionKey("1", "1"), "2", null, Partition.State.ASSIGNED)});
            Assert.assertNotNull(changeset);
            Assert.assertEquals(1, changeset.length);
            final Partition changed = changeset[0];
            Assert.assertEquals(new Partition.PartitionKey("1", "0"), changed.getKey());
            Assert.assertEquals("2", changed.getSession());
            Assert.assertEquals("3", changed.getNextSession());
            Assert.assertEquals(Partition.State.REASSIGNING, changed.getState());
        }
        {
            final Partition[] changeset = new ExactWeightRebalancer().apply(
                    new Session[]{new Session("1", 1), new Session("2", 1), new Session("3", 1)},
                    new Partition[] {
                            new Partition(new Partition.PartitionKey("0", "0"), "1", null, Partition.State.ASSIGNED),
                            new Partition(new Partition.PartitionKey("0", "1"), "1", null, Partition.State.ASSIGNED),
                            new Partition(new Partition.PartitionKey("1", "0"), "2", null, Partition.State.ASSIGNED),
                            new Partition(new Partition.PartitionKey("1", "1"), "2", null, Partition.State.REASSIGNING)});
            Assert.assertNotNull(changeset);
            Assert.assertEquals(1, changeset.length);
            final Partition changed = changeset[0];
            Assert.assertEquals(new Partition.PartitionKey("1", "1"), changed.getKey());
            Assert.assertEquals("2", changed.getSession());
            Assert.assertEquals("3", changed.getNextSession());
            Assert.assertEquals(Partition.State.REASSIGNING, changed.getState());
        }
    }
}