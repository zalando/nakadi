package de.zalando.aruha.nakadi.service.subscription.model;

import java.util.UUID;

public class Session {
    public final String id;
    public final int weight;

    public Session(final String id, final int weight) {
        assert null != id;
        this.id = id;
        this.weight = weight;
    }

    @Override
    public String toString() {
        return "Session{" + id + ", weight=" + weight + '}';
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        final Session session = (Session) o;

        if (weight != session.weight) return false;
        return id.equals(session.id);

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
