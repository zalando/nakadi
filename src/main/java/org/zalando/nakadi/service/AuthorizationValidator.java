package org.zalando.nakadi.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.domain.EventTypeResource;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.repository.EventTypeRepository;

@Service
public class AuthorizationValidator {

    private final AuthorizationService authorizationService;
    private final EventTypeRepository eventTypeRepository;

    @Autowired
    public AuthorizationValidator(
            final AuthorizationService authorizationService,
            final EventTypeRepository eventTypeRepository) {
        this.authorizationService = authorizationService;
        this.eventTypeRepository = eventTypeRepository;
    }

    public void validateAuthorization(@Nullable final EventTypeAuthorization auth) throws UnableProcessException,
            ServiceTemporarilyUnavailableException {
        if (auth != null) {
            final Map<String, List<AuthorizationAttribute>> allAttributes = ImmutableMap.of(
                    "admins", auth.getAdmins(),
                    "readers", auth.getReaders(),
                    "writers", auth.getWriters());
            checkAuthAttributesAreValid(allAttributes);
            checkAuthAttributesNoDuplicates(allAttributes);
        }
    }

    private void checkAuthAttributesNoDuplicates(final Map<String, List<AuthorizationAttribute>> allAttributes)
            throws UnableProcessException {
        final String duplicatesErrMessage = allAttributes.entrySet().stream()
                .map(entry -> {
                    final String property = entry.getKey();
                    final List<AuthorizationAttribute> attrs = entry.getValue();

                    final List<AuthorizationAttribute> duplicates = newArrayList(attrs);
                    final Set<AuthorizationAttribute> uniqueAttrs = newHashSet(attrs);
                    uniqueAttrs.forEach(duplicates::remove);

                    if (!duplicates.isEmpty()) {
                        final String duplicatesStr = duplicates.stream()
                                .distinct()
                                .map(attr -> String.format("%s:%s", attr.getDataType(), attr.getValue()))
                                .collect(Collectors.joining(", "));
                        final String err = String.format(
                                "authorization property '%s' contains duplicated attribute(s): %s",
                                property, duplicatesStr);
                        return Optional.of(err);
                    } else {
                        return Optional.<String>empty();
                    }
                })
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.joining("; "));

        if (!Strings.isNullOrEmpty(duplicatesErrMessage)) {
            throw new UnableProcessException(duplicatesErrMessage);
        }
    }

    private void checkAuthAttributesAreValid(final Map<String, List<AuthorizationAttribute>> allAttributes)
            throws UnableProcessException, ServiceTemporarilyUnavailableException {
        try {
            final String errorMessage = allAttributes.values().stream()
                    .flatMap(Collection::stream)
                    .filter(attr -> !authorizationService.isAuthorizationAttributeValid(attr))
                    .map(attr -> String.format("authorization attribute %s:%s is invalid",
                            attr.getDataType(), attr.getValue()))
                    .collect(Collectors.joining(", "));

            if (!Strings.isNullOrEmpty(errorMessage)) {
                throw new UnableProcessException(errorMessage);
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin", e);
        }
    }

    public void authorizeEventTypeWrite(final EventType eventType)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (eventType.getAuthorization() == null) {
            return;
        }
        final EventTypeResource resource = new EventTypeResource(
                eventType.getName(), eventType.getAuthorization());
        try {
            final boolean authorized = authorizationService.isAuthorized(
                    AuthorizationService.Operation.WRITE,
                    resource);
            if (!authorized) {
                throw new AccessDeniedException(AuthorizationService.Operation.WRITE, resource);
            }
        } catch (final PluginException ex) {
            throw new ServiceTemporarilyUnavailableException("Error while checking authorization", ex);
        }
    }

    public void authorizeEventTypeAdmin(final EventType eventType)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (eventType.getAuthorization() == null) {
            return;
        }

        final Resource resource = new EventTypeResource(eventType.getName(), eventType.getAuthorization());
        try {
            if (!authorizationService.isAuthorized(AuthorizationService.Operation.ADMIN, resource)) {
                throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource);
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin", e);
        }
    }

    public void authorizeStreamRead(final EventType eventType) throws AccessDeniedException {
        if (eventType.getAuthorization() == null) {
            return;
        }

        final Resource resource = new EventTypeResource(eventType.getName(), eventType.getAuthorization());
        try {
            if (!authorizationService.isAuthorized(AuthorizationService.Operation.READ, resource)) {
                throw new AccessDeniedException(AuthorizationService.Operation.READ, resource);
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin", e);
        }
    }

    public void authorizeSubscriptionRead(final SubscriptionBase subscriptionBase) throws AccessDeniedException {
        subscriptionBase.getEventTypes().forEach(
                (eventTypeName) -> {
                    try {
                        eventTypeRepository.findByNameO(eventTypeName).ifPresent(this::authorizeStreamRead);
                    } catch (final InternalNakadiException e) {
                        throw new ServiceTemporarilyUnavailableException(e);
                    }
                }
        );
    }

    public void validateAuthorization(final EventType original, final EventTypeBase newEventType)
            throws UnableProcessException, ServiceTemporarilyUnavailableException {
        final EventTypeAuthorization originalAuth = original.getAuthorization();
        final EventTypeAuthorization newAuth = newEventType.getAuthorization();
        if (originalAuth != null && newAuth == null) {
            throw new UnableProcessException(
                    "Changing authorization object to `null` is not possible due to existing one");
        }

        if (originalAuth != null && originalAuth.equals(newAuth)) {
            return;
        }

        validateAuthorization(newAuth);
    }

}
