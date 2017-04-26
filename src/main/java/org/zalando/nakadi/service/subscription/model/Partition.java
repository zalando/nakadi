package org.zalando.nakadi.service.subscription.model;

import java.util.Collection;
import javax.annotation.Nullable;
import org.zalando.nakadi.domain.TopicPartition;

public class Partition {
    public enum State {
        UNASSIGNED("unassigned"),
        REASSIGNING("reassigning"),
        ASSIGNED("assigned");

        private final String description;

        State(final String description) {
            this.description = description;
        }

        public String getDescription() {
            return description;
        }
    }

    private final TopicPartition key;
    private final String session;
    private final String nextSession;
    private final State state;

    public Partition(final TopicPartition key, @Nullable final String session, @Nullable final String nextSession,
                     final State state) {
        this.key = key;
        this.session = session;
        this.nextSession = nextSession;
        this.state = state;
    }

    public Partition toState(final State state, @Nullable final String session, @Nullable final String nextSession) {
        return new Partition(key, session, nextSession, state);
    }

    /**
     * Creates new Partition object that must be moved to session with id {@code sessionId}.
     *
     * @param sessionId          Session id to move to. It must be guaranteed that existingSessionIds do not contain
     *                           sessionId.
     * @param existingSessionIds List of currently available session ids.
     * @return new Partition object with changed sessionId, nextSessionId, state values.
     */
    public Partition moveToSessionId(final String sessionId, final Collection<String> existingSessionIds) {
        switch (state) {
            case UNASSIGNED:
                return toState(State.ASSIGNED, sessionId, null);
            case ASSIGNED:
            case REASSIGNING:
                if (sessionId.equals(this.session)) { // Just to be compliant with all possible cases.
                    return toState(State.ASSIGNED, sessionId, null);
                } else if (!existingSessionIds.contains(this.session)) {
                    return toState(State.ASSIGNED, sessionId, null);
                } else {
                    return toState(State.REASSIGNING, this.session, sessionId);
                }
            default:
                throw new IllegalStateException("Unsupported current state " + state);
        }
    }

    public boolean mustBeRebalanced(final Collection<String> activeSessionIds) {
        switch (state) {
            case UNASSIGNED:
                return true;
            case ASSIGNED:
                return !activeSessionIds.contains(session);
            case REASSIGNING:
                return !activeSessionIds.contains(session) || !activeSessionIds.contains(nextSession);
            default:
                throw new IllegalStateException("State of partition " + state + " is not supported");
        }

    }

    public TopicPartition getKey() {
        return key;
    }

    public State getState() {
        return state;
    }

    @Nullable
    public String getSession() {
        return session;
    }

    @Nullable
    public String getNextSession() {
        return nextSession;
    }

    @Nullable
    public String getSessionOrNextSession() {
        if (state == State.REASSIGNING) {
            return nextSession;
        }
        return session;
    }

    @Override
    public String toString() {
        return key + "->" + state + ":" + session + "->" + nextSession;
    }
}
