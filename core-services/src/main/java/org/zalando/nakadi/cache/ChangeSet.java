package org.zalando.nakadi.cache;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeSet {
    private Set<String> currentChanges = new HashSet<>();

    public boolean hasChanges(final List<Change> changeList) {
        final Set<String> newChangeIds = changeList.stream().map(Change::getId).collect(Collectors.toSet());
        return !currentChanges.equals(newChangeIds);
    }

    public Collection<String> getUpdatedEventTypes(final List<Change> newChanges) {
        final Set<String> eventTypes = new HashSet<>();
        for (final Change ch : newChanges) {
            if (!currentChanges.contains(ch.getId())) {
                eventTypes.add(ch.getEventTypeName());
            }
        }
        return eventTypes;
    }

    public void apply(final List<Change> newChanges) {
        this.currentChanges = newChanges.stream().map(Change::getId).collect(Collectors.toSet());
    }
}
