package org.zalando.nakadi.plugin.auth.subject;

import org.zalando.nakadi.plugin.api.authz.AuthorizationAttribute;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.plugin.auth.ZalandoTeamService;
import org.zalando.nakadi.plugin.auth.attribute.SimpleAuthorizationAttribute;
import org.zalando.nakadi.plugin.auth.attribute.TeamAuthorizationAttribute;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class EmployeeSubject extends UidSubject {

    private final ZalandoTeamService teamService;

    public EmployeeSubject(final String uid,
                           final Supplier<Set<String>> retailerIdsSupplier,
                           final String type,
                           final ZalandoTeamService teamService) {
        super(uid, retailerIdsSupplier, type);
        this.teamService = teamService;
    }


    public boolean isAuthorized(
            final String resourceType,
            final AuthorizationService.Operation operation,
            final Optional<List<AuthorizationAttribute>> attributes) {

        final List<AuthorizationAttribute> allAttributes = attributes.stream()
                .flatMap(Collection::stream)
                .collect(Collectors.toList());

        final List<AuthorizationAttribute> teamMembers = attributes.stream()
                .flatMap(Collection::stream)
                .filter(TeamAuthorizationAttribute::isTeamAuthorizationAttribute)
                .map(AuthorizationAttribute::getValue)
                .flatMap(team -> teamService.getTeamMembers(team).stream())
                .map(member -> new SimpleAuthorizationAttribute(type, member))
                .collect(Collectors.toList());

        allAttributes.addAll(teamMembers);

        return super.isAuthorized(resourceType, operation, Optional.of(allAttributes));

    }
}
