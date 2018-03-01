package org.zalando.nakadi.service.subscription;

public class IdleStreamWatcher {

    private final long idleCloseTimeout;

    private long idleStartMillis;
    private boolean isIdle;

    public IdleStreamWatcher(final long idleCloseTimeout) {
        this.idleCloseTimeout = idleCloseTimeout;
    }

    public long getIdleCloseTimeout() {
        return idleCloseTimeout;
    }

    public boolean idleStart() {
        if (!isIdle) {
            isIdle = true;
            idleStartMillis = System.currentTimeMillis();
            return true; // just started to be idle
        } else {
            return false; // was already idle
        }
    }

    public void idleEnd() {
        isIdle = false;
    }

    public boolean isIdleForToolLong() {
        final long currentMillis = System.currentTimeMillis();
        return isIdle && currentMillis >= idleStartMillis + idleCloseTimeout;
    }
}
