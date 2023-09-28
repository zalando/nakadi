package org.zalando.nakadi.service.subscription.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.zalando.nakadi.domain.EventTypePartition;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Objects;

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

    @JsonProperty("event_type")
    private String eventType;
    @JsonProperty("partition")
    private String partition;
    @JsonProperty("session")
    private String session;
    @JsonProperty("next_session")
    private String nextSession;
    @JsonProperty("state")
    private State state;
    // todo create data struct
    @JsonProperty("failed_commits_count")
    private int failedCommitsCount;
    @JsonProperty("last_dead_letter_offset") // when to stop looking for dead letter
    private String lastDeadLetterOffset;

    public Partition() {
    }

    public Partition(
            final String eventType,
            final String partition,
            @Nullable final String session,
            @Nullable final String nextSession,
            final State state) {
        this.eventType = eventType;
        this.partition = partition;
        this.session = session;
        this.nextSession = nextSession;
        this.state = state;
    }

    public Partition(
            final String eventType,
            final String partition,
            @Nullable final String session,
            @Nullable final String nextSession,
            final State state,
            final int failedCommitsCount,
            final String lastDeadLetterOffset) {
        this.eventType = eventType;
        this.partition = partition;
        this.session = session;
        this.nextSession = nextSession;
        this.state = state;
        this.failedCommitsCount = failedCommitsCount;
        this.lastDeadLetterOffset = lastDeadLetterOffset;
    }

    public Partition toState(final State state, @Nullable final String session, @Nullable final String nextSession) {
        return new Partition(eventType, partition, session, nextSession,
                state, failedCommitsCount, lastDeadLetterOffset);
    }

    public Partition toPartitionWithIncFailedCommits() {
        return new Partition(eventType, partition, session, nextSession,
                state, failedCommitsCount + 1, lastDeadLetterOffset);
    }

    public Partition toZeroFailedCommits() {
        return new Partition(eventType, partition, session, nextSession, state, 0, lastDeadLetterOffset);
    }

    public Partition toLookingDeadLetter(final String lastDeadLetterOffset) {

        if (lastDeadLetterOffset != null && this.lastDeadLetterOffset == null) {
            // failed commits reset to count for failures of the specific event
            return new Partition(eventType, partition, session, nextSession, state, 0, lastDeadLetterOffset);
        } else if (lastDeadLetterOffset == null && this.lastDeadLetterOffset != null) {
            return new Partition(eventType, partition, session, nextSession, state, 0, null);
        }

        return this;
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

    @JsonIgnore
    public EventTypePartition getKey() {
        return new EventTypePartition(eventType, partition);
    }

    public String getEventType() {
        return eventType;
    }

    public String getPartition() {
        return partition;
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
    @JsonIgnore
    public String getEffectiveSession() {
        if (state == State.REASSIGNING) {
            return nextSession;
        }
        return session;
    }

    @Override
    public String toString() {
        return eventType + ":" + partition + "->" + state + ":" +
                session + "->" + nextSession + ":" + failedCommitsCount + ":" + lastDeadLetterOffset;
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        final Partition partition1 = (Partition) o;
        return Objects.equals(eventType, partition1.eventType) &&
                Objects.equals(partition, partition1.partition) &&
                Objects.equals(session, partition1.session) &&
                Objects.equals(nextSession, partition1.nextSession) &&
                state == partition1.state;
    }

    @Override
    public int hashCode() {
        return Objects.hash(eventType, partition);
    }

    public int getFailedCommitsCount() {
        return failedCommitsCount;
    }

    public String getLastDeadLetterOffset() {
        return lastDeadLetterOffset;
    }

    @JsonIgnore
    public boolean isLookingForDeadLetter() {
        return lastDeadLetterOffset != null;
    }
}
