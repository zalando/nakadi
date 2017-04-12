package org.zalando.nakadi.util;

import com.google.common.collect.ImmutableSet;
import java.util.Set;

public class NakadiCollectionUtils {
    public static class Diff<T> {
        private final ImmutableSet<T> added;
        private final ImmutableSet<T> removed;

        Diff(final ImmutableSet<T> added, final ImmutableSet<T> removed) {
            this.added = added;
            this.removed = removed;
        }

        public ImmutableSet<T> getAdded() {
            return added;
        }

        public ImmutableSet<T> getRemoved() {
            return removed;
        }
    }

    public static <T> Diff<T> difference(final Set<T> oldSet, final Set<T> newSet) {
        final ImmutableSet.Builder<T> addedBuilder = ImmutableSet.builder();
        newSet.stream()
                .filter(v -> !oldSet.contains(v))
                .forEach(addedBuilder::add);
        final ImmutableSet.Builder<T> removedBuilder = ImmutableSet.builder();
        oldSet.stream()
                .filter(v -> !newSet.contains(v))
                .forEach(removedBuilder::add);
        return new Diff<>(addedBuilder.build(), removedBuilder.build());
    }
}
