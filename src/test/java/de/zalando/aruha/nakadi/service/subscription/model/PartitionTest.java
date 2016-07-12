package de.zalando.aruha.nakadi.service.subscription.model;

import java.util.Arrays;
import java.util.Collection;

import com.google.common.collect.ImmutableList;
import de.zalando.aruha.nakadi.service.subscription.model.Partition.PartitionKey;
import org.junit.Test;

import static de.zalando.aruha.nakadi.service.subscription.model.Partition.State.ASSIGNED;
import static de.zalando.aruha.nakadi.service.subscription.model.Partition.State.REASSIGNING;
import static de.zalando.aruha.nakadi.service.subscription.model.Partition.State.UNASSIGNED;
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
        assertTrue(new Partition(null, null, null, UNASSIGNED).mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, "T", null, UNASSIGNED).mustBeRebalanced(singletonList("T")));
        assertTrue(new Partition(null, "T", "T", UNASSIGNED).mustBeRebalanced(singletonList("T")));
    }

    @Test
    public void partitionShouldBeRebalancedIfOwnerIsBad() {
        final Collection<String> valid = singletonList("T");
        assertTrue(new Partition(null, null, null, ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", null, ASSIGNED).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", null, REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", "t", REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "T", "t", REASSIGNING).mustBeRebalanced(valid));
        assertTrue(new Partition(null, "t", "T", REASSIGNING).mustBeRebalanced(valid));
        assertFalse(new Partition(null, "T", "T", REASSIGNING).mustBeRebalanced(valid));
    }

    @Test
    public void moveToSessionIdUnassignedShouldProduceCorrectData() {
        final Collection<String> valid = singletonList("T");
        final PartitionKey pk = mock(PartitionKey.class);
        final Partition test = new Partition(pk, "x", "x", UNASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
        assertEquals(ASSIGNED, test.getState());
    }

    @Test
    public void moveReassigningPartitionShouldPutToAssignedState() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final PartitionKey pk = mock(PartitionKey.class);

        ImmutableList.of(
                new Partition(pk, "x", "x1", REASSIGNING),
                new Partition(pk, "x", "T", REASSIGNING),
                new Partition(pk, "T", "x", REASSIGNING),
                new Partition(pk, "T", "T1", REASSIGNING))
                .forEach(testPartition -> {
                    testPartition.moveToSessionId("T", validSessions);
                    assertSame(pk, testPartition.getKey());
                    assertEquals(ASSIGNED, testPartition.getState());
                    assertEquals("T", testPartition.getSession());
                    assertNull(testPartition.getNextSession());
                });
    }

    @Test
    public void moveReassigningPartitionShouldStayInReassigningStateWhenNextSessionIsTheSame() {
        final Collection<String> validSessions = Arrays.asList("T", "T1", "T2");
        final PartitionKey pk = mock(PartitionKey.class);
        final Partition test = new Partition(pk, "T1", "T2", REASSIGNING)
                .moveToSessionId("T", validSessions);
        assertSame(pk, test.getKey());
        assertEquals(REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToReassigningState() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final PartitionKey pk = mock(PartitionKey.class);
        final Partition test = new Partition(pk, "T1", null, ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        assertEquals(REASSIGNING, test.getState());
        assertEquals("T1", test.getSession());
        assertEquals("T", test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfOwnerSessionIsInvalid() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final PartitionKey pk = mock(PartitionKey.class);
        final Partition test = new Partition(pk, "x", null, ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        assertEquals(ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

    @Test
    public void moveAssignedShouldPutToAssignedStateIfMoveToSelf() {
        final Collection<String> valid = Arrays.asList("T", "T1");
        final PartitionKey pk = mock(PartitionKey.class);
        final Partition test = new Partition(pk, "T", null, ASSIGNED).moveToSessionId("T", valid);
        assertSame(pk, test.getKey());
        assertEquals(ASSIGNED, test.getState());
        assertEquals("T", test.getSession());
        assertNull(test.getNextSession());
    }

}
