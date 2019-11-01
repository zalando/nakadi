package org.zalando.nakadi.cache;

import java.util.List;

public class CacheChange {
    private final List<String> modified;
    private final List<String> deletedKeys;

    public CacheChange(final List<String> modified, final List<String> deletedKeys) {
        this.modified = modified;
        this.deletedKeys = deletedKeys;
    }

    public List<String> getModified() {
        return modified;
    }

    public List<String> getDeletedKeys() {
        return deletedKeys;
    }
}
