package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;
import org.zalando.nakadi.domain.AdminAuthorization;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AdminService {
    private final AuthorizationDbRepository authorizationDbRepository;
    private final TransactionTemplate transactionTemplate;

    public static final String ADMIN_RESOURCE = "nakadi";

    @Autowired
    public AdminService(final AuthorizationDbRepository authorizationDbRepository,
                        final TransactionTemplate transactionTemplate) {
        this.authorizationDbRepository = authorizationDbRepository;
        this.transactionTemplate = transactionTemplate;
    }

    public AdminAuthorization getAdmins() {
        final List<Permission> allPermissions = authorizationDbRepository.listAdmins();
        final List<AuthorizationAttribute> adminPermissions = allPermissions.stream()
                .filter(p -> p.getOperation() == AuthorizationService.Operation.ADMIN)
                .map(p -> p.getAuthorizationAttribute())
                .collect(Collectors.toList());
        final List<AuthorizationAttribute> readPermissions = allPermissions.stream()
                .filter(p -> p.getOperation() == AuthorizationService.Operation.READ)
                .map(p -> p.getAuthorizationAttribute())
                .collect(Collectors.toList());
        final List<AuthorizationAttribute> writePermissions = allPermissions.stream()
                .filter(p -> p.getOperation() == AuthorizationService.Operation.WRITE)
                .map(p -> p.getAuthorizationAttribute())
                .collect(Collectors.toList());
        return new AdminAuthorization(adminPermissions, readPermissions, writePermissions);
    }

    public void updateAdmins(final AdminAuthorization newAdmins) {
        final AdminAuthorization currentAdmins = getAdmins();
        for (final AuthorizationService.Operation operation : AuthorizationService.Operation.values()) {
            final List<Permission> delete = currentAdmins.getList(operation).stream()
                    .filter(t -> !newAdmins.getList(operation).stream().anyMatch(Predicate.isEqual(t)))
                    .map(attr -> new Permission(ADMIN_RESOURCE, operation, attr)).collect(Collectors.toList());
            final List<Permission> add = newAdmins.getList(operation).stream()
                    .filter(t -> !currentAdmins.getAdmins().stream().anyMatch(Predicate.isEqual(t)))
                    .map(attr -> new Permission(ADMIN_RESOURCE, operation, attr)).collect(Collectors.toList());
            authorizationDbRepository.update(add, delete);
        }
    }
}
