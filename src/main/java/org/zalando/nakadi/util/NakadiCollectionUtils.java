package org.zalando.nakadi.util;

import java.util.Set;
import java.util.stream.Collectors;

public class NakadiCollectionUtils {
    public static class Diff<T> {
        public final Set<T> added;
        public final Set<T> removed;

        Diff(final Set<T> added, final Set<T> removed) {
            this.added = added;
            this.removed = removed;
        }
    }

    public static <T> Diff<T> difference(final Set<T> oldSet, final Set<T> newSet) {
        final Set<T> added = newSet.stream()
                .filter(v -> !oldSet.contains(v))
                .collect(Collectors.toSet());
        final Set<T> removed = oldSet.stream()
                .filter(v -> !newSet.contains(v))
                .collect(Collectors.toSet());
        return new Diff<>(added, removed);
    }
}
