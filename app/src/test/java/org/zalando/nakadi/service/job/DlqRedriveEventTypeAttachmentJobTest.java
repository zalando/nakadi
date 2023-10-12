package org.zalando.nakadi.service.job;

import org.junit.Assert;
import org.junit.Test;
import org.zalando.nakadi.service.subscription.model.Partition;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DlqRedriveEventTypeAttachmentJobTest {
    @Test
    public void whenAddingDlqToTopologyThenOnlyMissingPartitionsAreAdded() {
        final Partition[] existingPartitions = new Partition[] {
            new Partition("abc", "0", null, null, Partition.State.UNASSIGNED),
            new Partition("abc", "1", "session-1", "session-2", Partition.State.ASSIGNED),
            new Partition("dlq-redrive", "0", "session-1", "session-2", Partition.State.ASSIGNED)
        };
        final List<Partition> unassignedDlqPartitions = List.of(
                new Partition("dlq-redrive", "0", null, null, Partition.State.UNASSIGNED),
                new Partition("dlq-redrive", "1", null, null, Partition.State.UNASSIGNED));

        final Partition[] newPartitions =
                DlqRedriveEventTypeAttachmentJob.addPartitionsIfMissing(
                        existingPartitions,
                        unassignedDlqPartitions);

        final Set<Partition> newPartitionsSet = new HashSet<>();
        Collections.addAll(newPartitionsSet, newPartitions);

        Assert.assertEquals(
                Set.of(
                        new Partition("abc", "0", null, null, Partition.State.UNASSIGNED),
                        new Partition("abc", "1", "session-1", "session-2", Partition.State.ASSIGNED),
                        new Partition("dlq-redrive", "0", "session-1", "session-2", Partition.State.ASSIGNED),
                        new Partition("dlq-redrive", "1", null, null, Partition.State.UNASSIGNED)),
                newPartitionsSet);
    }
}
