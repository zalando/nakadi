package org.zalando.nakadi.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.Permission;
import org.zalando.nakadi.repository.db.AuthorizationDbRepository;

import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Service
public class AdminService {
    private final AuthorizationDbRepository authorizationDbRepository;

    @Autowired
    public AdminService(final AuthorizationDbRepository authorizationDbRepository) {
        this.authorizationDbRepository = authorizationDbRepository;
    }

    public List<Permission> getAdmins() {
        return authorizationDbRepository.listAdmins();
    }

    public void updateAdmins(final List<Permission> newAdmins) {
        final List<Permission> currentAdmins = getAdmins();
        final List<Permission> add = newAdmins.stream()
                .filter(p -> !currentAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList());
        final List<Permission> delete = currentAdmins.stream()
                .filter(p -> !newAdmins.stream().anyMatch(Predicate.isEqual(p))).collect(Collectors.toList());
        authorizationDbRepository.update(add, delete);
    }
}
