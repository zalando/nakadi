package org.zalando.nakadi.service;

import java.util.List;
import java.util.Optional;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

class MyEventTypeResource2 implements Resource {
    private final String name;
    private final EventTypeAuthorization etAuthorization;

    MyEventTypeResource2(final String name, final EventTypeAuthorization etAuthorization) {
        this.name = name;
        this.etAuthorization = etAuthorization;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public String getType() {
        return "event-type";
    }

    @Override
    public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
            final AuthorizationService.Operation operation) {
        switch (operation) {
            case READ:
                return Optional.of(etAuthorization.getReaders());
            case WRITE:
                return Optional.of(etAuthorization.getWriters());
            case ADMIN:
                return Optional.of(etAuthorization.getAdmins());
            default:
                throw new IllegalArgumentException("Operation " + operation + " is not supported");
        }
    }

    @Override
    public String toString() {
        return "AuthorizedResource{event-type='" + name + "}";
    }
}
