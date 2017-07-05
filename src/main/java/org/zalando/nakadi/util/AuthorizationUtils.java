package org.zalando.nakadi.util;

import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporaryUnavailableException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.security.Client;

import java.util.List;
import java.util.Optional;

public class AuthorizationUtils {
    public static Resource createStreamResource(final EventType eventType) {
        return new Resource() {
            @Override
            public String getName() {
                return eventType.getName();
            }

            @Override
            public String getType() {
                return "event-type";
            }

            @Override
            public Optional<List<AuthorizationAttribute>> getAttributesForOperation(
                    final AuthorizationService.Operation operation) {
                return Optional.ofNullable(eventType.getAuthorization()).map(EventTypeAuthorization::getReaders);
            }
        };
    }

    public static void authorizeStreamRead(final AuthorizationService authorizationService, final Client client,
                                     final EventType eventType) throws AccessDeniedException {
        final Resource resource = AuthorizationUtils.createStreamResource(eventType);
        final Subject subject = AuthorizationUtils.createSubject(client);
        if (!authorizationService.isAuthorized(subject, AuthorizationService.Operation.READ, resource)) {
            throw new AccessDeniedException(subject, AuthorizationService.Operation.READ, resource);
        }
    }

    public static void authorizeSubscriptionRead(final EventTypeRepository eventTypeRepository,
                                                 final AuthorizationService authorizationService,
                                                 final Client client, final SubscriptionBase subscriptionBase) {
        subscriptionBase.getEventTypes().stream().forEach(
                (eventTypeName) -> {
                    try {
                        eventTypeRepository.findByNameO(eventTypeName).ifPresent(eventType -> {
                            authorizeStreamRead(authorizationService, client, eventType);
                        });
                    } catch (final InternalNakadiException e) {
                        throw new ServiceTemporaryUnavailableException(e);
                    }
                }
        );
    }

    public static Subject createSubject(final Client client) {
        return null;
    }

    public static String errorMessage(final AccessDeniedException e) {
        return new StringBuilder()
                .append("Access on ")
                .append(e.getOperation())
                .append(" ")
                .append(e.getResource().getType())
                .append(":")
                .append(e.getResource().getName())
                .append(" denied")
                .toString();
    }
}
