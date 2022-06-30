package org.zalando.nakadi.service;

import com.google.common.base.Strings;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.cache.EventTypeCache;
import org.zalando.nakadi.domain.BatchItem;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.NakadiRecord;
import org.zalando.nakadi.domain.NakadiRecordResource;
import org.zalando.nakadi.domain.Subscription;
import org.zalando.nakadi.exceptions.runtime.AccessDeniedException;
import org.zalando.nakadi.exceptions.runtime.ForbiddenOperationException;
import org.zalando.nakadi.exceptions.runtime.InternalNakadiException;
import org.zalando.nakadi.exceptions.runtime.ServiceTemporarilyUnavailableException;
import org.zalando.nakadi.exceptions.runtime.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.UnprocessableEntityException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.plugin.api.exceptions.AuthorizationInvalidException;
import org.zalando.nakadi.plugin.api.exceptions.OperationOnResourceNotPermittedException;
import org.zalando.nakadi.plugin.api.exceptions.PluginException;

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
    private final EventTypeCache eventTypeCache;
    private final AdminService adminService;

    @Autowired
    public AuthorizationValidator(
            final AuthorizationService authorizationService,
            final EventTypeCache eventTypeCache,
            final AdminService adminService) {
        this.authorizationService = authorizationService;
        this.adminService = adminService;
        this.eventTypeCache = eventTypeCache;
    }

    public void validateAuthorization(final Resource resource) throws UnableProcessException,
            ServiceTemporarilyUnavailableException {
        checkAuthorisationForResourceAreValid(resource);
        if (resource.getAuthorization() != null) {
            checkAuthAttributesNoDuplicates(resource.getAuthorization());
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

    private void checkAuthorisationForResourceAreValid(final Resource resource)
            throws UnableProcessException, ServiceTemporarilyUnavailableException, ForbiddenOperationException {

        try {
            authorizationService.isAuthorizationForResourceValid(resource);
        } catch (OperationOnResourceNotPermittedException e) {
            throw new ForbiddenOperationException(e.getMessage());
        } catch (AuthorizationInvalidException e) {
            throw new UnprocessableEntityException(e.getMessage());
        } catch (PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin: "
                    + e.getMessage(), e);
        }
    }


    public void authorizeEventTypeWrite(final EventType eventType)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (eventType.getAuthorization() == null) {
            return;
        }
        final Resource<EventType> resource = eventType.asResource();
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

    public void authorizeEventWrite(final NakadiRecord nakadiRecord)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (nakadiRecord.getOwner() == null) {
            return;
        }
        try {
            final var nakadiRecordResource = new NakadiRecordResource(nakadiRecord);
            final boolean authorized = authorizationService.isAuthorized(
                    AuthorizationService.Operation.WRITE, nakadiRecordResource);
            if (!authorized) {
                throw new AccessDeniedException(AuthorizationService.Operation.WRITE, nakadiRecordResource);
            }
        } catch (final PluginException ex) {
            throw new ServiceTemporarilyUnavailableException("Error while checking authorization", ex);
        }
    }

    public void authorizeEventWrite(final BatchItem batchItem)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (batchItem.getOwner() == null) {
            return;
        }
        try {
            final boolean authorized = authorizationService.isAuthorized(
                    AuthorizationService.Operation.WRITE, batchItem);
            if (!authorized) {
                throw new AccessDeniedException(AuthorizationService.Operation.WRITE, batchItem);
            }
        } catch (final PluginException ex) {
            throw new ServiceTemporarilyUnavailableException("Error while checking authorization", ex);
        }
    }

    private void authorizeResourceAdmin(final Resource resource) throws AccessDeniedException {
        try {
            if (!authorizationService.isAuthorized(AuthorizationService.Operation.ADMIN, resource)) {
                if (!adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
                    throw new AccessDeniedException(AuthorizationService.Operation.ADMIN, resource);
                }
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin: "
                    + e.getMessage(), e);
        }
    }

    private void authorizeResourceView(final Resource resource) throws AccessDeniedException {
        try {
            if (!(adminService.isAdmin(AuthorizationService.Operation.ADMIN) ||
                    authorizationService.isAuthorized(AuthorizationService.Operation.VIEW, resource))) {
                throw new AccessDeniedException(AuthorizationService.Operation.VIEW, resource);
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin: "
                    + e.getMessage(), e);
        }
    }

    public void authorizeEventTypeView(final EventType eventType)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        authorizeResourceView(eventType.asResource());
    }

    public void authorizeSubscriptionView(final Subscription subscription)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        authorizeResourceView(subscription.asResource());
    }

    public void authorizeEventTypeAdmin(final EventType eventType)
            throws AccessDeniedException, ServiceTemporarilyUnavailableException {
        if (eventType.getAuthorization() == null) {
            return;
        }
        authorizeResourceAdmin(eventType.asResource());
    }

    public void authorizeStreamRead(final EventType eventType) throws AccessDeniedException {
        if (eventType.getAuthorization() == null) {
            return;
        }

        final Resource resource = eventType.asResource();
        checkResourceAuthorization(resource);
    }

    public void authorizeSubscriptionRead(final Subscription subscription) throws AccessDeniedException {
        if (null != subscription.getAuthorization()) {
            final Resource resource = subscription.asResource();
            checkResourceAuthorization(resource);
        }
        subscription.getEventTypes().forEach(
                (eventTypeName) -> {
                    try {
                        eventTypeCache.getEventTypeIfExists(eventTypeName).ifPresent(this::authorizeStreamRead);
                    } catch (final InternalNakadiException e) {
                        throw new ServiceTemporarilyUnavailableException(e);
                    }
                }
        );
    }

    public void authorizeSubscriptionCommit(final Subscription subscription) throws AccessDeniedException {
        if (null == subscription.getAuthorization()) {
            return;
        }
        final Resource resource = subscription.asResource();
        checkResourceAuthorization(resource);

    }

    private void checkResourceAuthorization(final Resource resource)
            throws ServiceTemporarilyUnavailableException, AccessDeniedException {
        try {
            if (!authorizationService.isAuthorized(AuthorizationService.Operation.READ, resource)
                    && !adminService.hasAllDataAccess(AuthorizationService.Operation.READ)) {
                throw new AccessDeniedException(AuthorizationService.Operation.READ, resource);
            }
        } catch (final PluginException e) {
            throw new ServiceTemporarilyUnavailableException("Error calling authorization plugin: "
                    + e.getMessage(), e);
        }
    }

    public void authorizeSubscriptionAdmin(final Subscription subscription) throws AccessDeniedException {
        if (subscription.getAuthorization() == null) {
            return;
        }
        authorizeResourceAdmin(subscription.asResource());
    }

    public void validateAuthorization(final Resource oldValue, final Resource newValue)
            throws UnableProcessException, ServiceTemporarilyUnavailableException {
        final Map<String, List<AuthorizationAttribute>> oldAuth = oldValue.getAuthorization();
        final Map<String, List<AuthorizationAttribute>> newAuth = newValue.getAuthorization();
        if (oldAuth != null && newAuth == null) {
            throw new UnableProcessException(
                    "Changing authorization object to `null` is not possible due to existing one");
        }
        validateAuthorization(newValue);
    }
}
