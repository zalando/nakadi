package org.zalando.nakadi.service.subscription.model;

import java.util.UUID;

public class Session {
    private final String id;
    private final int weight;

    public Session(final String id, final int weight) {
        this.id = id;
        this.weight = weight;
    }

    public String getId() {
        return id;
    }

    public int getWeight() {
        return weight;
    }

    @Override
    public String toString() {
        return "Session{" + id + ", weight=" + weight + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final Session session = (Session) o;
        return weight == session.getWeight() && id.equals(session.getId());
    }

    @Override
    public int hashCode() {
        int result = id.hashCode();
        result = 31 * result + weight;
        return result;
    }

    public static Session generate(final int weight) {
        return new Session(UUID.randomUUID().toString(), weight);
    }
}
