package de.zalando.aruha.nakadi.util;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Function;

public class TimeBasedCache<Key, Value> {
    private static class ValueWithTime<Value> {
        private final Value value;
        private final long addedAt;

        public ValueWithTime(final Value value, final long addedAt) {
            this.value = value;
            this.addedAt = addedAt;
        }

    }

    private final LinkedHashMap<Key, ValueWithTime<Value>> holder = new LinkedHashMap<>();
    private final ReadWriteLock rwLock = new ReentrantReadWriteLock();
    private final long delayMillis;

    public TimeBasedCache(final long delayMillis) {
        this.delayMillis = delayMillis;
    }

    public Value getOrCalculate(final Key key, final Function<Key, Value> valueCalculator) {
        removeOldValues();
        rwLock.readLock().lock();
        try {
            final ValueWithTime<Value> result = holder.get(key);
            if (result != null) {
                return result.value;
            }
        } finally {
            rwLock.readLock().unlock();
        }
        final Value value = valueCalculator.apply(key);
        final long currentTime = System.currentTimeMillis();
        rwLock.writeLock().lock();
        try {
            return holder.computeIfAbsent(key, (k) -> new ValueWithTime<>(value, currentTime)).value;
        } finally {
            rwLock.writeLock().unlock();
        }
    }

    private void removeOldValues() {
        final long lastRecordMs = System.currentTimeMillis() - delayMillis;
        rwLock.writeLock().lock();
        try {
            final Iterator<Map.Entry<Key, ValueWithTime<Value>>> iterator = holder.entrySet().iterator();
            while (iterator.hasNext()) {
                final Map.Entry<Key, ValueWithTime<Value>> entry = iterator.next();
                if (entry.getValue().addedAt < lastRecordMs) {
                    iterator.remove();
                } else {
                    break;
                }
            }
        } finally {
            rwLock.writeLock().unlock();
        }
    }
}
