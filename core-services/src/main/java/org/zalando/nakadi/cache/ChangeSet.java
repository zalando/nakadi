package org.zalando.nakadi.cache;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ChangeSet {
    private Map<String, List<Change>> currentChanges = new HashMap<>();

    public Collection<String> getUpdatedEventTypes(final List<Change> newChanges) {
        final Set<String> eventTypes = new HashSet<>();
        for (final Change ch : newChanges) {
            final List<Change> currentEtChanges = currentChanges.get(ch.getEventTypeName());
            if (currentEtChanges == null) {
                // Event type is first time in the list
                eventTypes.add(ch.getEventTypeName());
            } else {
                final boolean alreadyExists = currentEtChanges.stream().anyMatch(c -> c.getId().equals(ch.getId()));
                if (!alreadyExists) {
                    eventTypes.add(ch.getEventTypeName());
                }
            }
        }
        return eventTypes;
    }

    /**
     * Because all the changes are exist in 2 forms - zookeeper form (with unknown creation date) and stored form,
     * we have to pick up change date from the stored values. If it's not there, then consider change received time as
     * actual one.
     * @param change Change to get occurrence time.
     * @return moment of change being first time seen by this change set.
     */
    private Date getActualChangeDate(final Change change) {
        final List<Change> currentEtChanges = currentChanges.get(change.getEventTypeName());
        if (currentEtChanges == null) {
            return change.getOccurredAt();
        }
        return currentEtChanges.stream()
                .filter(ch -> ch.getId().equals(change.getId()))
                .map(Change::getOccurredAt)
                .sorted(Comparator.reverseOrder())
                .findAny()
                .orElse(change.getOccurredAt());
    }

    public Collection<Change> getChangesToRemove(final List<Change> newChanges, final long changeTTLms) {
        // There are 2 reasons to remove:
        // 1. Event type is present several times
        // 2. Change is too old.
        final Map<String, List<Change>> newChangeMap = newChanges.stream()
                .collect(Collectors.groupingBy(Change::getEventTypeName));
        final List<Change> toDelete = new ArrayList<>();
        for (final Map.Entry<String, List<Change>> newEntry : newChangeMap.entrySet()) {
            // figure out proper dates of the changes
            final List<Change> newChangesSorted = newEntry.getValue().stream()
                    .sorted(Comparator.comparing(this::getActualChangeDate).thenComparing(Change::getId).reversed())
                    .collect(Collectors.toList());
            final long newestAge = System.currentTimeMillis() - getActualChangeDate(newChangesSorted.get(0)).getTime();
            if (newestAge > changeTTLms) {
                toDelete.addAll(newChangesSorted);
            } else if (newChangesSorted.size() > 1) {
                toDelete.addAll(newChangesSorted.subList(1, newChangesSorted.size()));
            }
        }
        return toDelete;
    }

    public void apply(final List<Change> newChanges) {
        this.currentChanges = newChanges.stream()
                .map(c -> new Change(c.getId(), c.getEventTypeName(), getActualChangeDate(c)))
                .collect(Collectors.groupingBy(Change::getEventTypeName));
    }
}
