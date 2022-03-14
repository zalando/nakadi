package org.zalando.nakadi.validation.schema;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@Service
public class CompatibilityModeChangeConstraint implements SchemaEvolutionConstraint {
    final Map<CompatibilityMode, List<CompatibilityMode>> allowedChanges = ImmutableMap.of(
            CompatibilityMode.COMPATIBLE, Lists.newArrayList(CompatibilityMode.COMPATIBLE),
            CompatibilityMode.FORWARD, Lists.newArrayList(CompatibilityMode.FORWARD, CompatibilityMode.COMPATIBLE),
            CompatibilityMode.NONE, Lists.newArrayList(CompatibilityMode.NONE, CompatibilityMode.FORWARD)
    );

    private final AdminService adminService;
    private final AuthorizationService authorizationService;

    @Autowired
    public CompatibilityModeChangeConstraint(final AdminService adminService,
                                             final AuthorizationService authorizationService) {
        this.adminService = adminService;
        this.authorizationService = authorizationService;
    }

    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        final boolean isNakadiAdmin = adminService.isAdmin(AuthorizationService.Operation.WRITE);
        final boolean isEventTypeAdmin = authorizationService
                .isAuthorized(AuthorizationService.Operation.ADMIN, original.asResource());
        final boolean isChangeValid = allowedChanges.get(original.getCompatibilityMode())
                .contains(eventType.getCompatibilityMode());
        if (isEventTypeAdmin || isNakadiAdmin || isChangeValid) {
            return Optional.empty();
        } else {
            return Optional.of(new SchemaEvolutionIncompatibility("changing compatibility_mode is not allowed"));
        }

    }
}
