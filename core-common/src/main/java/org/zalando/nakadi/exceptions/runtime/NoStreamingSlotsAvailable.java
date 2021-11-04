package org.zalando.nakadi.exceptions.runtime;

public class NoStreamingSlotsAvailable extends NakadiBaseException {
    public NoStreamingSlotsAvailable(final String subscriptionId, final int totalSlots) {
        super(String.format("No free slots for streaming available for subscription %s. Total slots: %d",
                subscriptionId, totalSlots));
    }
}
