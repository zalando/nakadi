package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.exceptions.UnableProcessException;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AdminService {
    private final AuthorizationDbRepository authorizationDbRepository;
    private final AuthorizationService authorizationService;

    @Autowired
    public AdminService(final AuthorizationDbRepository authorizationDbRepository,
                        final AuthorizationService authorizationService) {
        this.authorizationDbRepository = authorizationDbRepository;
        this.authorizationService = authorizationService;
    }

    public List<Permission> getAdmins() {
        return authorizationDbRepository.listAdmins();
    }

    public void updateAdmins(final List<Permission> newAdmins) throws UnableProcessException {
        validateAllAdmins(newAdmins);
        final List<Permission> currentAdmins = getAdmins();
        final List<Permission> add = newAdmins.stream()
                .filter(p -> !currentAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList());
        final List<Permission> delete = currentAdmins.stream()
                .filter(p -> !newAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList());
        authorizationDbRepository.update(add, delete);
    }

    private void validateAllAdmins(final List<Permission> admins) throws UnableProcessException {
        final List<Permission> invalid = admins.stream().filter(permission ->
                !authorizationService.isAuthorizationAttributeValid(permission.getAuthorizationAttribute()))
                .collect(Collectors.toList());
        if (!invalid.isEmpty()) {
            String message = invalid.stream()
                    .map(permission -> String.format("authorization attribute %s:%s is invalid",
                            permission.getAuthorizationAttribute().getDataType(),
                            permission.getAuthorizationAttribute().getValue()))
                    .collect(Collectors.joining(", "));
            throw new UnableProcessException(message);
        }
    }
}
