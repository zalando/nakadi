package org.zalando.nakadi.util;

import java.util.Set;
import java.util.stream.Collectors;

public class NakadiUtils {
    public static class Diff<T> {
        public final Set<T> added;
        public final Set<T> removed;

        Diff(final Set<T> added, final Set<T> removed) {
            this.added = added;
            this.removed = removed;
        }
    }

    public static <T> Diff<T> difference(final Set<T> oldCollection, final Set<T> newCollection) {
        final Set<T> added = newCollection.stream()
                .filter(v -> !oldCollection.contains(v))
                .collect(Collectors.toSet());
        final Set<T> removed = oldCollection.stream()
                .filter(v -> !newCollection.contains(v))
                .collect(Collectors.toSet());
        return new Diff<>(added, removed);
    }
}
