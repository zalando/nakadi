package org.zalando.nakadi.service.subscription.model;

import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.EventTypePartition;

import java.util.Arrays;
import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class PartitionTest {
    @Test
    public void partitionShouldBeRebalancedIfItsFree() {
        assertTrue(new Partition(null, null, null, null, Partition.State.UNASSIGNED)
                .mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, null, "T", null, Partition.State.UNASSIGNED)
                .mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, null, "T", "T", Partition.State.UNASSIGNED)
                .mustBeRebalanced(singletonList("T")));
    }

    @Test
    public void partitionShouldBeRebalancedIfOwnerIsBad() {
        final Collection<String> valid = singletonList("T");
        assertTrue(new Partition(null, null, null, null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, null, "t", null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, null, "t", null, Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, null, "t", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, null, "T", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, null, "t", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertFalse(new Partition(null, null, "T", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
    }

    @Test
    public void moveToSessionIdUnassignedShouldProduceCorrectData() {
        final Collection<String> valid = singletonList("T");
        final String eventType = "et";
        final String partition = "partition";
        final Partition test = new Partition(eventType, partition, "x", "x", Partition.State.UNASSIGNED)
                .moveToSessionId("T", valid);
        assertEquals(new EventTypePartition(eventType, partition), test.getKey());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
    }

    @Test
    public void moveReassigningPartitionShouldPutToAssignedState() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final String eventType = "et";
        final String partition = "partition";

        ImmutableList.of(
                new Partition(eventType, partition, "x", "x1", Partition.State.REASSIGNING),
                new Partition(eventType, partition, "x", "T", Partition.State.REASSIGNING),
                new Partition(eventType, partition, "T", "x", Partition.State.REASSIGNING),
                new Partition(eventType, partition, "T", "T1", Partition.State.REASSIGNING))
                .forEach(testPartition -> {
                    final Partition movedPartition = testPartition.moveToSessionId("T", validSessions);
                    assertEquals(new EventTypePartition(eventType, partition), movedPartition.getKey());
                    Assert.assertEquals(Partition.State.ASSIGNED, movedPartition.getState());
                    assertEquals("T", movedPartition.getSession());
                    assertNull(movedPartition.getNextSession());
                });
    }

    @Test
    public void moveReassigningPartitionShouldStayInReassigningStateWhenNextSessionIsTheSame() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final String eventType = "et";
        final String partition = "partition";
        final Partition test = new Partition(eventType, partition, "T1", "T2", Partition.State.REASSIGNING)
                .moveToSessionId("T", validSessions);
        assertEquals(new EventTypePartition(eventType, partition), test.getKey());
        Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToReassigningState() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final String eventType = "et";
        final String partition = "partition";
        final Partition test = new Partition(eventType, partition, "T1", null, Partition.State.ASSIGNED)
                .moveToSessionId("T", valid);
        assertEquals(new EventTypePartition(eventType, partition), test.getKey());
        Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfOwnerSessionIsInvalid() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final String eventType = "et";
        final String partition = "partition";
        final Partition test = new Partition(eventType, partition, "x", null, Partition.State.ASSIGNED)
                .moveToSessionId("T", valid);
        assertEquals(new EventTypePartition(eventType, partition), test.getKey());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfMoveToSelf() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final String eventType = "et";
        final String partition = "partition";
        final Partition test = new Partition(eventType, partition, "T", null, Partition.State.ASSIGNED)
                .moveToSessionId("T", valid);
        assertEquals(new EventTypePartition(eventType, partition), test.getKey());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

}
