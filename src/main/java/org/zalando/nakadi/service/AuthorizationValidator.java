package org.zalando.nakadi.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.SubscriptionBase;
import org.zalando.nakadi.exceptions.InternalNakadiException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporaryUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.authz.Subject;
import org.zalando.nakadi.repository.EventTypeRepository;
import org.zalando.nakadi.security.Client;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import static com.google.common.collect.Lists.newArrayList;
import static com.google.common.collect.Sets.newHashSet;

@Service
public class AuthorizationValidator {

    private final AuthorizationService authorizationService;

    @Autowired
    public AuthorizationValidator(final AuthorizationService authorizationService) {
        this.authorizationService = authorizationService;
    }

    public void validateAuthorization(@Nullable final EventTypeAuthorization auth) throws InvalidEventTypeException,
            ServiceUnavailableException {
        if (auth != null) {
            final Map<String, List<AuthorizationAttribute>> allAttributes = ImmutableMap.of(
                    "admins", auth.getAdmins(),
                    "readers", auth.getReaders(),
                    "writers", auth.getWriters());
            checkAuthAttributesAreValid(allAttributes);
            checkAuthAttributesNoDuplicates(allAttributes);
        }
    }

    public void authorizeStreamRead(final Client client, final EventType eventType) throws AccessDeniedException {
        final Resource resource = createStreamResource(eventType);
        final Subject subject = null;
        if (!authorizationService.isAuthorized(subject, AuthorizationService.Operation.READ, resource)) {
            throw new AccessDeniedException(subject, AuthorizationService.Operation.READ, resource);
        }
    }

    public void authorizeSubscriptionRead(final EventTypeRepository eventTypeRepository, final Client client,
                                          final SubscriptionBase subscriptionBase) throws AccessDeniedException {
        subscriptionBase.getEventTypes().stream().forEach(
                (eventTypeName) -> {
                    try {
                        eventTypeRepository.findByNameO(eventTypeName).ifPresent(eventType -> {
                            authorizeStreamRead(client, eventType);
                        });
                    } catch (final InternalNakadiException e) {
                        throw new ServiceTemporaryUnavailableException(e);
                    }
                }
        );
    }

    private Resource createStreamResource(final EventType eventType) {
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

    private void checkAuthAttributesNoDuplicates(final Map<String, List<AuthorizationAttribute>> allAttributes)
            throws InvalidEventTypeException {
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
            throw new InvalidEventTypeException(duplicatesErrMessage);
        }
    }

    private void checkAuthAttributesAreValid(final Map<String, List<AuthorizationAttribute>> allAttributes)
            throws InvalidEventTypeException, ServiceUnavailableException {
        try {
            final String errorMessage = allAttributes.values().stream()
                    .flatMap(Collection::stream)
                    .filter(attr -> !authorizationService.isAuthorizationAttributeValid(attr))
                    .map(attr -> String.format("authorization attribute %s:%s is invalid",
                            attr.getDataType(), attr.getValue()))
                    .collect(Collectors.joining(", "));

            if (!Strings.isNullOrEmpty(errorMessage)) {
                throw new InvalidEventTypeException(errorMessage);
            }
        } catch (final PluginException e) {
            throw new ServiceUnavailableException("Error calling authorization plugin", e);
        }
    }
}
