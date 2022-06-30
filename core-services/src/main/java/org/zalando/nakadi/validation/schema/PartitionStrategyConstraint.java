package org.zalando.nakadi.validation.schema;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.domain.EventTypeBase;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;

import java.util.Optional;

@Service
public class PartitionStrategyConstraint implements SchemaEvolutionConstraint {
    private final AdminService adminService;

    @Autowired
    public PartitionStrategyConstraint(final AdminService adminService) {
        this.adminService = adminService;
    }

    @Override
    public Optional<SchemaEvolutionIncompatibility> validate(final EventType original, final EventTypeBase eventType) {
        if (adminService.isAdmin(AuthorizationService.Operation.WRITE)) {
            // allow nakadi administrators to bypass the partitioning strategy check
            return Optional.empty();
        }
        if (!original.getPartitionStrategy().equals(PartitionStrategy.RANDOM_STRATEGY)
                && !eventType.getPartitionStrategy().equals(original.getPartitionStrategy())) {
            return Optional.of(new SchemaEvolutionIncompatibility("changing partition_strategy " +
                    "is only allowed if the original strategy was 'random'"));
        } else {
            return Optional.empty();
        }
    }
}