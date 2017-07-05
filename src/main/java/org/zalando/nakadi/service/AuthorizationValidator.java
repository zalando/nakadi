package org.zalando.nakadi.service;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeAuthorization;
import org.zalando.nakadi.domain.EventTypeResource;
import org.zalando.nakadi.exceptions.ForbiddenAccessException;
import org.zalando.nakadi.exceptions.InvalidEventTypeException;
import org.zalando.nakadi.exceptions.ServiceUnavailableException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.plugin.api.PluginException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;

import javax.annotation.Nullable;
import java.util.Collection;
import java.util.Collections;
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

    public void authorizeEventTypeUpdate(final EventType eventType)
            throws ForbiddenAccessException, ServiceTemporarilyUnavailableException {
        if (eventType.getAuthorization() == null) {
            return;
        }

        final Resource resource = new EventTypeResource(eventType.getName(), "event-type",
                Collections.singletonMap(AuthorizationService.Operation.ADMIN,
                        eventType.getAuthorization().getAdmins()));
        try {
            if (!authorizationService.isAuthorized(null, AuthorizationService.Operation.ADMIN, resource)) {
                throw new ForbiddenAccessException("Updating the `EventType` is only allowed for clients that " +
                        "satisfy the authorization `admin` requirements");
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin", e);
        }
    }
}
