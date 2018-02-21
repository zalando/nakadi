package org.zalando.nakadi.service.subscription;

public class IdleStreamWatcher {

    private final long idleCloseTimeout;

    private volatile long idleStartMillis;
    private volatile boolean isIdle;

    public IdleStreamWatcher(final long idleCloseTimeout) {
        this.idleCloseTimeout = idleCloseTimeout;
    }

    public long getIdleCloseTimeout() {
        return idleCloseTimeout;
    }

    public synchronized boolean idleStart() {
        if (!isIdle) {
            isIdle = true;
            idleStartMillis = System.currentTimeMillis();
            return true; // just started to be idle
        } else {
            return false; // was already idle
        }
    }

    public synchronized void idleEnd() {
        isIdle = false;
    }

    public synchronized boolean isIdleForToolLong() {
        final long currentMillis = System.currentTimeMillis();
        return isIdle && currentMillis >= idleStartMillis + idleCloseTimeout;
    }
}
