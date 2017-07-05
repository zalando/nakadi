package org.zalando.nakadi.domain;

import org.zalando.nakadi.plugin.api.authz.Subject;

public class EventTypeSubject implements Subject {

    private final String name;
    private final String token;

    public EventTypeSubject(final String name, final String token) {
        this.name = name;
        this.token = token;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getToken() {
        return token;
    }
}
