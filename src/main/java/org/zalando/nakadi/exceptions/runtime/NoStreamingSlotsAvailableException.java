package org.zalando.nakadi.exceptions.runtime;

public class NoStreamingSlotsAvailableException extends NakadiRuntimeBaseException {
    public NoStreamingSlotsAvailableException(final int totalSlots) {
        super("No free slots for streaming available. Total slots: " + totalSlots);
    }
}
