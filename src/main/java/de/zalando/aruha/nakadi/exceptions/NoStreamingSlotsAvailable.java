package de.zalando.aruha.nakadi.exceptions;

import javax.ws.rs.core.Response;

public class NoStreamingSlotsAvailable extends NakadiException {
    public NoStreamingSlotsAvailable(final int totalSlots) {
        super("No free slots for streaming available. Total slots: " + totalSlots);
    }

    @Override
    protected Response.StatusType getStatus() {
        return Response.Status.BAD_REQUEST;
    }
}
