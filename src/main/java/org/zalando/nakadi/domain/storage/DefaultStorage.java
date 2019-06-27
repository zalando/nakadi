package org.zalando.nakadi.domain.storage;

public class DefaultStorage {

    private volatile Storage storage;

    public DefaultStorage(final Storage storage) {
        this.storage = storage;
    }

    public void setStorage(final Storage storage) {
        this.storage = storage;
    }

    public Storage getStorage() {
        return storage;
    }
}
