package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.config.NakadiSettings;
import org.zalando.nakadi.domain.AdminResource;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.domain.ResourceAuthorization;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.api.authz.Resource;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.zalando.nakadi.domain.AdminResource.ADMIN_RESOURCE;

@Service
public class AdminService {
    private final AuthorizationDbRepository authorizationDbRepository;
    private final AuthorizationService authorizationService;
    private final NakadiSettings nakadiSettings;

    @Autowired
    public AdminService(final AuthorizationDbRepository authorizationDbRepository,
                        final AuthorizationService authorizationService, final NakadiSettings nakadiSettings) {
        this.authorizationDbRepository = authorizationDbRepository;
        this.authorizationService = authorizationService;
        this.nakadiSettings = nakadiSettings;
    }

    public List<Permission> getAdmins() {
        return addDefaultAdmin(authorizationDbRepository.listAdmins());
    }

    public void updateAdmins(final List<Permission> newAdmins) {
        final List<Permission> currentAdmins = getAdmins();
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
        for (final AuthorizationService.Operation operation: AuthorizationService.Operation.values()) {
            if (permissions.stream().noneMatch(p -> p.getOperation().equals(operation))) {
                permissions.add(new Permission(ADMIN_RESOURCE, operation, nakadiSettings.getDefaultAdmin()));
            }
        }
        return permissions;
    }

    private List<Permission> removeDefaultAdmin(final List<Permission> permissions) {
        return permissions.stream()
                .filter(p -> !p.getAuthorizationAttribute().equals(nakadiSettings.getDefaultAdmin()))
                .collect(Collectors.toList());
    }
}
