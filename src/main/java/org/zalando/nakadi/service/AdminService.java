package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.AdminResource;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.exceptions.runtime.DbWriteOperationsBlockedException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;
import org.zalando.nakadi.util.FeatureToggleService;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.AdminResource.ADMIN_RESOURCE;

@Service
public class AdminService {

    private final AuthorizationDbRepository authorizationDbRepository;
    private final AuthorizationService authorizationService;
    private final FeatureToggleService featureToggleService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public AdminService(final AuthorizationDbRepository authorizationDbRepository,
                        final AuthorizationService authorizationService,
                        final FeatureToggleService featureToggleService,
                        final NakadiSettings nakadiSettings) {
        this.authorizationDbRepository = authorizationDbRepository;
        this.authorizationService = authorizationService;
        this.featureToggleService = featureToggleService;
        this.nakadiSettings = nakadiSettings;
    }

    public List<Permission> getAdmins() {
        return addDefaultAdmin(authorizationDbRepository.listAdmins());
    }

    public void updateAdmins(final List<Permission> newAdmins) {
        if (featureToggleService.isFeatureEnabled(FeatureToggleService.Feature.DISABLE_DB_WRITE_OPERATIONS)) {
            throw new DbWriteOperationsBlockedException("Cannot update admins: write operations on DB " +
                    "are blocked by feature flag.");
        }
        validateAllAdmins(newAdmins);
        final List<Permission> currentAdmins = authorizationDbRepository.listAdmins();
        final List<Permission> add = removeDefaultAdmin(newAdmins.stream()
                .filter(p -> !currentAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList()));
        final List<Permission> delete = removeDefaultAdmin(currentAdmins.stream()
                .filter(p -> !newAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList()));
        authorizationDbRepository.update(add, delete);
    }

    public boolean isAdmin(final AuthorizationService.Operation operation) {
        final List<Permission> permissions = getAdmins();
        final Resource resource = new AdminResource(ADMIN_RESOURCE,
                ResourceAuthorization.fromPermissionsList(permissions));
        return authorizationService.isAuthorized(operation, resource);
    }

    private List<Permission> addDefaultAdmin(final List<Permission> permissions) {
        for (final AuthorizationService.Operation operation : AuthorizationService.Operation.values()) {
            permissions.add(new Permission(ADMIN_RESOURCE, operation, nakadiSettings.getDefaultAdmin()));
        }
        return permissions;
    }

    private List<Permission> removeDefaultAdmin(final List<Permission> permissions) {
        return permissions.stream()
                .filter(p -> !p.getAuthorizationAttribute().equals(nakadiSettings.getDefaultAdmin()))
                .collect(Collectors.toList());
    }

    private void validateAllAdmins(final List<Permission> admins) throws UnableProcessException {
        final List<Permission> invalid = admins.stream().filter(permission ->
                !authorizationService.isAuthorizationAttributeValid(permission.getAuthorizationAttribute()))
                .collect(Collectors.toList());
        if (!invalid.isEmpty()) {
            final String message = invalid.stream()
                    .map(permission -> String.format("authorization attribute %s:%s is invalid",
                            permission.getAuthorizationAttribute().getDataType(),
                            permission.getAuthorizationAttribute().getValue()))
                    .collect(Collectors.joining(", "));
            throw new UnableProcessException(message);
        }
    }
}
