package de.zalando.aruha.nakadi.service.subscription.model;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class PartitionTest {
    @Test
    public void partitionShouldBeRebalancedIfItsFree() {
        Assert.assertTrue(new Partition(null, null, null, Partition.State.UNASSIGNED).mustBeRebalanced(Collections.singletonList("T")));
        Assert.assertTrue(new Partition(null, "T", null, Partition.State.UNASSIGNED).mustBeRebalanced(Collections.singletonList("T")));
        Assert.assertTrue(new Partition(null, "T", "T", Partition.State.UNASSIGNED).mustBeRebalanced(Collections.singletonList("T")));
    }

    @Test
    public void partitionShouldBeRebalancedIfOwnerIsBad() {
        final Collection<String> valid = Collections.singletonList("T");
        Assert.assertTrue(new Partition(null, null, null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        Assert.assertTrue(new Partition(null, "t", null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        Assert.assertTrue(new Partition(null, "t", null, Partition.State.REASSIGNING).mustBeRebalanced(valid));
        Assert.assertTrue(new Partition(null, "t", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        Assert.assertTrue(new Partition(null, "T", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        Assert.assertTrue(new Partition(null, "t", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        Assert.assertFalse(new Partition(null, "T", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
    }

    @Test
    public void moveToSessionIdUnassignedShouldProduceCorrectData() {
        final Collection<String> valid = Collections.singletonList("T");
        final Partition.PartitionKey pk = Mockito.mock(Partition.PartitionKey.class);
        final Partition test = new Partition(pk, "x", "x", Partition.State.UNASSIGNED).moveToSessionId("T", valid);
        Assert.assertSame(pk, test.getKey());
        Assert.assertEquals("T", test.getSession());
        Assert.assertNull(test.getNextSession());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
    }

    @Test
    public void moveToSessionIdReassigningShouldProduceCorrectData() {
        final Collection<String> valid = Arrays.asList("T", "T1", "T2");
        final Partition.PartitionKey pk = Mockito.mock(Partition.PartitionKey.class);
        {
            final Partition test = new Partition(pk, "x", "x1", Partition.State.REASSIGNING).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "x", "T", Partition.State.REASSIGNING).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "T", "x", Partition.State.REASSIGNING).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "T", "T1", Partition.State.REASSIGNING).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "T1", "T2", Partition.State.REASSIGNING).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
            Assert.assertEquals("T1", test.getSession());
            Assert.assertEquals("T", test.getNextSession());
        }
    }
    @Test
    public void moveToSessionIdAssignedShouldProduceCorrectData() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final Partition.PartitionKey pk = Mockito.mock(Partition.PartitionKey.class);
        {
            final Partition test = new Partition(pk, "T1", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
            Assert.assertEquals("T1", test.getSession());
            Assert.assertEquals("T", test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "x", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
        {
            final Partition test = new Partition(pk, "T", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
            Assert.assertSame(pk, test.getKey());
            Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
            Assert.assertEquals("T", test.getSession());
            Assert.assertNull(test.getNextSession());
        }
    }
}