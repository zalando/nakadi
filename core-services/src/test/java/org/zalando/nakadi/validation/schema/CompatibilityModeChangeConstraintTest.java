package org.zalando.nakadi.validation.schema;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;
import org.zalando.nakadi.domain.CompatibilityMode;
import org.zalando.nakadi.domain.EventType;
import org.zalando.nakadi.plugin.api.authz.AuthorizationService;
import org.zalando.nakadi.service.AdminService;
import org.zalando.nakadi.utils.EventTypeTestBuilder;
import org.zalando.nakadi.utils.IsOptional;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;

public class CompatibilityModeChangeConstraintTest {
    @Test
    public void cannotDowngradeCompatibilityMode() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(false),
                eventTypeAdmin(false));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isPresent());
    }

    @Test
    public void adminsCanDowngradeCompatibilityMode() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(true),
                eventTypeAdmin(false));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void eventTypeAdminsCanDowngradeCompatibilityMode() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(false),
                eventTypeAdmin(true));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void canPromoteFromForwardToCompatible() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.FORWARD).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.COMPATIBLE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(false),
                eventTypeAdmin(false));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void canPromoteFromNoneToForward() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.FORWARD).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(false),
                eventTypeAdmin(false));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    @Test
    public void passWhenNoChanges() throws Exception {
        final EventTypeTestBuilder builder = new EventTypeTestBuilder();
        final EventType oldET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final EventType newET = builder.compatibilityMode(CompatibilityMode.NONE).build();
        final CompatibilityModeChangeConstraint constraint = new CompatibilityModeChangeConstraint(adminMock(false),
                eventTypeAdmin(false));

        Assert.assertThat(constraint.validate(oldET, newET), IsOptional.isAbsent());
    }

    private AdminService adminMock(final boolean isAdmin) {
        final AdminService adminService = mock(AdminService.class);
        Mockito.when(adminService.isAdmin(AuthorizationService.Operation.WRITE)).thenReturn(isAdmin);
        return adminService;
    }

    private AuthorizationService eventTypeAdmin(final boolean isAdmin) {
        final AuthorizationService authorizationService = mock(AuthorizationService.class);
        Mockito.when(authorizationService.isAuthorized(any(), any()))
                .thenReturn(isAdmin);
        return authorizationService;
    }
}