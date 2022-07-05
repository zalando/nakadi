package org.zalando.nakadi.validation.schema;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.mockito.stubbing.OngoingStubbing;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.partitioning.PartitionStrategy;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;

import java.util.Objects;
import java.util.Optional;

import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartitionStrategyConstraintTest {
    @Mock
    private AdminService adminService;

    private PartitionStrategyConstraint constraint;

    private static final String[] ALL_STRATEGIES = {
            PartitionStrategy.HASH_STRATEGY,
            PartitionStrategy.RANDOM_STRATEGY,
            PartitionStrategy.USER_DEFINED_STRATEGY};

    @Before
    public void before() {
        constraint = new PartitionStrategyConstraint(adminService);
    }

    @Test
    public void migrationFromRandomToAnythingIsAllowed() {
        when(adminService.isAdmin(eq(AuthorizationService.Operation.WRITE))).thenReturn(false);
        final EventType original = new EventType();
        original.setPartitionStrategy(PartitionStrategy.RANDOM_STRATEGY);

        for (final String value : ALL_STRATEGIES) {
            final EventType changed = new EventType();
            changed.setPartitionStrategy(value);

            final Optional<SchemaEvolutionIncompatibility> error = constraint.validate(original, changed);
            Assert.assertFalse(error.isPresent());
        }
    }

    @Test
    public void migrationFromNonRandomToAnythingIsForbidden() {
        when(adminService.isAdmin(eq(AuthorizationService.Operation.WRITE))).thenReturn(false);

        for (final String o : new String[]{PartitionStrategy.HASH_STRATEGY, PartitionStrategy.USER_DEFINED_STRATEGY}) {
            final EventType original = new EventType();
            original.setPartitionStrategy(o);
            for (final String t : ALL_STRATEGIES) {
                if (Objects.equals(t, o)) {
                    continue;
                }
                final EventType target = new EventType();
                target.setPartitionStrategy(t);
                final Optional<SchemaEvolutionIncompatibility> error = constraint.validate(original, target);
                Assert.assertTrue(error.isPresent());
            }
        }

    }

    @Test
    public void migrationFromAnythingToAnythingForAdminAllowed() {
        OngoingStubbing<Boolean> stubbing = when(adminService.isAdmin(eq(AuthorizationService.Operation.WRITE)));
        for (final String o : ALL_STRATEGIES) {
            final EventType original = new EventType();
            original.setPartitionStrategy(o);
            for (final String t : ALL_STRATEGIES) {
                final EventType target = new EventType();
                target.setPartitionStrategy(t);

                stubbing = stubbing.thenReturn(true);

                final Optional<SchemaEvolutionIncompatibility> error = constraint.validate(original, target);
                Assert.assertFalse(error.isPresent());
            }
        }
    }
}