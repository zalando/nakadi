package org.zalando.nakadi.cache;

import java.util.List;

public class CacheChange {
    private final List<String> modifiedKeys;
    private final List<String> deletedKeys;

    public CacheChange(final List<String> modifiedKeys, final List<String> deletedKeys) {
        this.modifiedKeys = modifiedKeys;
        this.deletedKeys = deletedKeys;
    }

    public List<String> getModifiedKeys() {
        return modifiedKeys;
    }

    public List<String> getDeletedKeys() {
        return deletedKeys;
    }
}
