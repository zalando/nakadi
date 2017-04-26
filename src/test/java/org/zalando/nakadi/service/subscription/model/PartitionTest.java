package org.zalando.nakadi.service.subscription.model;

import com.google.common.collect.ImmutableList;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.domain.TopicPartition;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class PartitionTest {
    @Test
    public void partitionShouldBeRebalancedIfItsFree() {
        assertTrue(new Partition(null, null, null, Partition.State.UNASSIGNED).mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, "T", null, Partition.State.UNASSIGNED).mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, "T", "T", Partition.State.UNASSIGNED).mustBeRebalanced(singletonList("T")));
    }

    @Test
    public void partitionShouldBeRebalancedIfOwnerIsBad() {
        final Collection<String> valid = singletonList("T");
        assertTrue(new Partition(null, null, null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", null, Partition.State.ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", null, Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "T", "t", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
        assertFalse(new Partition(null, "T", "T", Partition.State.REASSIGNING).mustBeRebalanced(valid));
    }

    @Test
    public void moveToSessionIdUnassignedShouldProduceCorrectData() {
        final Collection<String> valid = singletonList("T");
        final TopicPartition pk = mock(TopicPartition.class);
        final Partition test = new Partition(pk, "x", "x", Partition.State.UNASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
    }

    @Test
    public void moveReassigningPartitionShouldPutToAssignedState() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final TopicPartition pk = mock(TopicPartition.class);

        ImmutableList.of(
                new Partition(pk, "x", "x1", Partition.State.REASSIGNING),
                new Partition(pk, "x", "T", Partition.State.REASSIGNING),
                new Partition(pk, "T", "x", Partition.State.REASSIGNING),
                new Partition(pk, "T", "T1", Partition.State.REASSIGNING))
                .forEach(testPartition -> {
                    final Partition movedPartition = testPartition.moveToSessionId("T", validSessions);
                    assertSame(pk, movedPartition.getKey());
                    Assert.assertEquals(Partition.State.ASSIGNED, movedPartition.getState());
                    assertEquals("T", movedPartition.getSession());
                    assertNull(movedPartition.getNextSession());
                });
    }

    @Test
    public void moveReassigningPartitionShouldStayInReassigningStateWhenNextSessionIsTheSame() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final TopicPartition pk = mock(TopicPartition.class);
        final Partition test = new Partition(pk, "T1", "T2", Partition.State.REASSIGNING)
                .moveToSessionId("T", validSessions);
        assertSame(pk, test.getKey());
        Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToReassigningState() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final TopicPartition pk = mock(TopicPartition.class);
        final Partition test = new Partition(pk, "T1", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        Assert.assertEquals(Partition.State.REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfOwnerSessionIsInvalid() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final TopicPartition pk = mock(TopicPartition.class);
        final Partition test = new Partition(pk, "x", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfMoveToSelf() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final TopicPartition pk = mock(TopicPartition.class);
        final Partition test = new Partition(pk, "T", null, Partition.State.ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        Assert.assertEquals(Partition.State.ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

}
