package org.zalando.nakadi.service;

import java.util.List;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.zalando.nakadi.config.SecuritySettings;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.IllegalScopeException;
import org.zalando.nakadi.exceptions.ResourceAccessNotAuthorizedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.security.Client;

@Component
public class AuthzChecker {
    private abstract static class Checker {
        abstract void check(AuthorizationService.Operation operation)
                throws IllegalScopeException, ResourceAccessNotAuthorizedException, ForbiddenAccessException;
    }

    private final AuthorizationService authorizationService;
    private final SecuritySettings securitySettings;

    @Autowired
    public AuthzChecker(final AuthorizationService authorizationService, final SecuritySettings securitySettings) {
        this.authorizationService = authorizationService;
        this.securitySettings = securitySettings;
    }

    public void performCheck(final EventType eventType, final AuthorizationService.Operation operation,
                             final Client client) {
        final Checker checker;
        if (null == eventType.getAuthorization()) {
            checker = new OldChecker(client, securitySettings, eventType);
        } else {
            checker = new NewChecker(
                    authorizationService,
                    new EventTypeAuthzResource(eventType.getName(), eventType.getAuthorization()));
        }
        checker.check(operation);
    }

    private static final class EventTypeAuthzResource implements Resource {
        private final String name;
        private final EventTypeAuthorization etAuthorization;

        private EventTypeAuthzResource(final String name, final EventTypeAuthorization etAuthorization) {
            this.name = name;
            this.etAuthorization = etAuthorization;
        }

        @Override
        public String getName() {
            return name;
        }

        @Override
        public String getType() {
            return "EventType";
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

    private static class OldChecker extends Checker {
        private final Client client;
        private final SecuritySettings securitySettings;
        private final EventType eventType;

        private OldChecker(final Client client, final SecuritySettings securitySettings, final EventType eventType) {
            this.client = client;
            this.securitySettings = securitySettings;
            this.eventType = eventType;
        }

        @Override
        void check(final AuthorizationService.Operation operation)
                throws IllegalScopeException, ForbiddenAccessException {
            switch (operation) {
                case READ:
                    client.checkScopes(eventType.getReadScopes());
                    break;
                case WRITE:
                    client.checkScopes(eventType.getWriteScopes());
                    break;
                case ADMIN:
                    if (!client.getClientId().equals(securitySettings.getAdminClientId())) {
                        throw new ForbiddenAccessException("Request is forbidden for user " + client.getClientId());
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Operation " + operation + " is not supported");
            }
        }
    }

    private static class NewChecker extends Checker {
        private final AuthorizationService authorizationService;
        private final Resource resource;

        NewChecker(final AuthorizationService authorizationService, final Resource resource) {
            this.authorizationService = authorizationService;
            this.resource = resource;
        }

        @Override
        void check(final AuthorizationService.Operation operation) throws ResourceAccessNotAuthorizedException {
            final boolean allow = authorizationService.isAuthorized(null, operation, resource);
            if (!allow) {
                throw new ResourceAccessNotAuthorizedException(operation, resource);
            }
        }
    }
}
